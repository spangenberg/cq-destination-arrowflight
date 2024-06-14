package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the client for the Apache Arrow destination
type Client struct {
	logger zerolog.Logger
	spec   Spec

	done               chan struct{}
	flightClient       flight.Client
	flightDoPutClients []flight.FlightService_DoPutClient
	lock               sync.RWMutex
	wg                 sync.WaitGroup
	writers            map[string]*flight.Writer

	plugin.UnimplementedSource
}

var _ plugin.Client = (*Client)(nil)

// New creates a new Apache Arrow destination client
func New(ctx context.Context, logger zerolog.Logger, specBytes []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger:  logger.With().Str("module", "arrowflight").Logger(),
		done:    make(chan struct{}),
		writers: make(map[string]*flight.Writer),
	}
	if opts.NoConnection {
		return c, nil
	}

	if err := json.Unmarshal(specBytes, &c.spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal spec: %w", err)
	}
	c.spec.SetDefaults()

	var callOptions []grpc.CallOption
	if c.spec.MaxCallRecvMsgSize != nil {
		callOptions = append(callOptions, grpc.MaxCallRecvMsgSize(*c.spec.MaxCallRecvMsgSize))
	}
	if c.spec.MaxCallSendMsgSize != nil {
		callOptions = append(callOptions, grpc.MaxCallSendMsgSize(*c.spec.MaxCallSendMsgSize))
	}

	var err error
	if c.flightClient, err = flight.NewClientWithMiddlewareCtx(ctx, c.spec.Addr, NewAuthHandler(c.spec.Handshake, c.spec.Token), nil, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(callOptions...)); err != nil {
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}
	if c.spec.Handshake != "" {
		if err = c.flightClient.Authenticate(ctx); err != nil {
			return nil, fmt.Errorf("failed to authenticate flight client: %w", err)
		}
	}

	return c, nil
}

// Read is called when a table is read
func (c *Client) Read(ctx context.Context, table *schema.Table, res chan<- arrow.Record) error {
	c.logger.Debug().Str("table", table.Name).Msg("read")
	flightInfo, err := c.getFlightInfo(ctx, table.Name)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}
	var reader *ipc.Reader
	if reader, err = ipc.NewReader(bytes.NewReader(flightInfo.GetSchema())); err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()
	for _, endpoint := range flightInfo.GetEndpoint() {
		if err = c.doGet(ctx, endpoint, reader.Schema(), res); err != nil {
			return err
		}
	}
	return nil
}

// Write receives messages from the plugin and writes them to the destination
func (c *Client) Write(ctx context.Context, messages <-chan message.WriteMessage) error {
	for msg := range messages {
		switch v := msg.(type) {
		case *message.WriteMigrateTable:
			if err := c.MigrateTable(ctx, v); err != nil {
				return fmt.Errorf("failed to migrate table: %w", err)
			}
		case *message.WriteInsert:
			if err := c.Insert(ctx, v); err != nil {
				return fmt.Errorf("failed to insert: %w", err)
			}
		case *message.WriteDeleteStale:
			if err := c.DeleteStale(ctx, v); err != nil {
				return fmt.Errorf("failed to delete stale: %w", err)
			}
		case *message.WriteDeleteRecord:
			if err := c.DeleteRecord(ctx, v); err != nil {
				return fmt.Errorf("failed to delete record: %w", err)
			}
		default:
			return fmt.Errorf("unknown message type: %T", v)
		}
	}
	return nil
}

// Close is called when the client is closed
func (c *Client) Close(context.Context) error {
	if c.flightClient == nil {
		return fmt.Errorf("client already closed or not initialized")
	}

	close(c.done)

	c.logger.Debug().Msg("closing writers")
	for _, writer := range c.writers {
		if err := writer.Close(); err != nil {
			c.logger.Error().Err(err).Msg("failed to close writer")
		}
	}

	waitTimeout(&c.wg, c.spec.CloseTimeout.Duration())

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
