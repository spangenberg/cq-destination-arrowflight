package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spangenberg/cq-destination-arrowflight/client/spec"
)

type Client struct {
	flightClient flight.Client
	logger       zerolog.Logger
	mutex        sync.RWMutex
	spec         spec.Spec
	writers      map[string]*Writer

	plugin.UnimplementedSource
}

// Assert Client implements plugin.Client interface.
var _ plugin.Client = (*Client)(nil)

func New(ctx context.Context, logger zerolog.Logger, specBytes []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger:  logger.With().Str("module", "arrowflight").Logger(),
		writers: make(map[string]*Writer),
	}
	if opts.NoConnection {
		return c, nil
	}

	if err := json.Unmarshal(specBytes, &c.spec); err != nil {
		return nil, err
	}
	if err := c.spec.Validate(); err != nil {
		return nil, err
	}
	c.spec.SetDefaults()

	if err := c.connect(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) Close(context.Context) error {
	if err := c.closeWriters(); err != nil {
		return fmt.Errorf("failed to close writers: %w", err)
	}

	c.logger.Info().Msg("closing flight client")
	if err := c.flightClient.Close(); err != nil {
		return fmt.Errorf("failed to close flight client: %w", err)
	}

	c.flightClient = nil

	return nil
}

func (c *Client) authenticate(ctx context.Context) error {
	if len(c.spec.Handshake) == 0 {
		return nil
	}

	c.logger.Info().Msg("authenticating flight client")
	if err := c.flightClient.Authenticate(ctx); err != nil {
		return fmt.Errorf("failed to authenticate flight client: %w", err)
	}

	return nil
}

func (c *Client) closeWriters() error {
	c.logger.Info().Msg("closing writers")

	c.logger.Debug().Msg("acquiring lock to close writers")
	c.mutex.Lock()
	c.logger.Debug().Msg("acquired lock to close writers")
	defer c.mutex.Unlock()

	var errs []error
	for _, writer := range c.writers {
		if err := writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("failed to close writers: %w", err)
	}

	clear(c.writers)

	return nil
}

func (c *Client) connect(ctx context.Context) error {
	c.logger.Info().Msg("connecting flight client")

	c.logger.Debug().Msg("acquiring lock to connect flight client")
	c.mutex.Lock()
	c.logger.Debug().Msg("acquired lock to connect flight client")
	defer c.mutex.Unlock()

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

	{
		var err error
		if c.flightClient, err = flight.NewClientWithMiddlewareCtx(ctx, c.spec.Addr, newAuthHandler(c.spec.Handshake, c.spec.Token), nil, grpcDialOptions...); err != nil {
			return fmt.Errorf("failed to create flight client: %w", err)
		}
	}

	if err := c.authenticate(ctx); err != nil {
		return fmt.Errorf("failed to authenticate flight client: %w", err)
	}

	return nil
}
