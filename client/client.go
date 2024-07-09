package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/writers/mixedbatchwriter"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spangenberg/cq-destination-arrowflight/client/spec"
)

type Client struct {
	logger             zerolog.Logger
	writer             *mixedbatchwriter.MixedBatchWriter
	lock               sync.RWMutex
	flightClient       flight.Client
	flightDoPutClients []flight.FlightService_DoPutClient
	writers            map[string]*flight.Writer

	plugin.UnimplementedSource
}

// Assert Client implements plugin.Client interface.
var _ plugin.Client = (*Client)(nil)

func New(ctx context.Context, logger zerolog.Logger, specBytes []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger:  logger.With().Str("module", "arrowflight").Logger(),
		writers: make(map[string]*flight.Writer),
	}
	if opts.NoConnection {
		return c, nil
	}

	var s spec.Spec
	if err := json.Unmarshal(specBytes, &s); err != nil {
		return nil, err
	}
	s.SetDefaults()
	if err := s.Validate(); err != nil {
		return nil, err
	}

	var transportCredentials credentials.TransportCredentials
	if s.TlsEnabled {
		transportCredentials = credentials.NewTLS(&tls.Config{
			ServerName:         s.TlsServerName,
			InsecureSkipVerify: s.TlsInsecureSkipVerify,
		})
	} else {
		transportCredentials = insecure.NewCredentials()
	}
	grpcDialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(s.MaxCallRecvMsgSize),
			grpc.MaxCallSendMsgSize(s.MaxCallSendMsgSize),
		),
	}

	var err error
	if c.flightClient, err = flight.NewClientWithMiddlewareCtx(ctx, s.Addr, newAuthHandler(s.Handshake, s.Token), nil, grpcDialOptions...); err != nil {
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}
	if len(s.Handshake) > 0 {
		if err = c.flightClient.Authenticate(ctx); err != nil {
			return nil, fmt.Errorf("failed to authenticate flight client: %w", err)
		}
	}

	c.writer, err = mixedbatchwriter.New(c,
		mixedbatchwriter.WithLogger(c.logger),
		mixedbatchwriter.WithBatchSize(s.BatchSize),
		mixedbatchwriter.WithBatchSizeBytes(s.BatchSizeBytes),
		mixedbatchwriter.WithBatchTimeout(s.BatchTimeout.Duration()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create mixed batch writer: %w", err)
	}

	return c, nil
}

func (c *Client) Write(ctx context.Context, res <-chan message.WriteMessage) error {
	return c.writer.Write(ctx, res)
}

func (c *Client) Close(context.Context) error {
	if c.flightClient == nil {
		return fmt.Errorf("client already closed or not initialized")
	}

	c.logger.Debug().Msg("closing writers")
	for _, writer := range c.writers {
		if err := writer.Close(); err != nil {
			c.logger.Error().Err(err).Msg("failed to close writer")
		}
	}

	c.logger.Debug().Msg("closing do put clients")
	for _, flightDoPutClient := range c.flightDoPutClients {
		if err := flightDoPutClient.CloseSend(); err != nil {
			c.logger.Error().Err(err).Msg("failed to close send")
		}
	}

	c.logger.Info().Msg("closing flight client")
	if err := c.flightClient.Close(); err != nil {
		c.logger.Error().Err(err).Msg("failed to close flight client")
	}

	c.flightClient = nil
	return nil
}
