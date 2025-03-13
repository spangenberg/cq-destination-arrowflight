package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight"
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
	spec               spec.Spec

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

	if err := json.Unmarshal(specBytes, &c.spec); err != nil {
		return nil, err
	}
	c.spec.SetDefaults()
	if err := c.spec.Validate(); err != nil {
		return nil, err
	}

	if err := c.reconnect(ctx); err != nil {
		return nil, err
	}

	{
		var err error
		if c.writer, err = mixedbatchwriter.New(c,
			mixedbatchwriter.WithLogger(c.logger),
			mixedbatchwriter.WithBatchSize(c.spec.BatchSize),
			mixedbatchwriter.WithBatchSizeBytes(c.spec.BatchSizeBytes),
			mixedbatchwriter.WithBatchTimeout(c.spec.BatchTimeout.Duration()),
		); err != nil {
			return nil, fmt.Errorf("failed to create mixed batch writer: %w", err)
		}
	}

	return c, nil
}

func (c *Client) reconnect(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	_ = c.Close(ctx)

	var transportCredentials credentials.TransportCredentials
	if c.spec.TlsEnabled {
		transportCredentials = credentials.NewTLS(&tls.Config{
			ServerName:         c.spec.TlsServerName,
			InsecureSkipVerify: c.spec.TlsInsecureSkipVerify,
		})
	} else {
		transportCredentials = insecure.NewCredentials()
	}
	grpcDialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.spec.MaxCallRecvMsgSize),
			grpc.MaxCallSendMsgSize(c.spec.MaxCallSendMsgSize),
		),
	}

	c.logger.Info().Msg("connecting to flight server")
	{
		var err error
		if c.flightClient, err = flight.NewClientWithMiddlewareCtx(ctx, c.spec.Addr, newAuthHandler(c.spec.Handshake, c.spec.Token), nil, grpcDialOptions...); err != nil {
			return fmt.Errorf("failed to create flight client: %w", err)
		}
	}
	c.logger.Info().Msg("connected to flight server")

	if len(c.spec.Handshake) == 0 {
		return nil
	}

	c.logger.Info().Msg("authenticating flight client")
	if err := c.flightClient.Authenticate(ctx); err != nil {
		return fmt.Errorf("failed to authenticate flight client: %w", err)
	}
	c.logger.Info().Msg("authenticated flight client")

	return nil
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
	clear(c.writers)

	c.logger.Debug().Msg("closing do put clients")
	for _, flightDoPutClient := range c.flightDoPutClients {
		if err := flightDoPutClient.CloseSend(); err != nil {
			c.logger.Error().Err(err).Msg("failed to close send")
		}
	}
	c.flightDoPutClients = nil

	c.logger.Info().Msg("closing flight client")
	if err := c.flightClient.Close(); err != nil {
		c.logger.Error().Err(err).Msg("failed to close flight client")
	}

	c.flightClient = nil
	return nil
}
