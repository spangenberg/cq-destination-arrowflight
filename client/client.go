package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
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

	flightClient       flight.Client
	flightDoPutClients []flight.FlightService_DoPutClient
	writers            map[string]*flight.Writer
	writersLock        sync.RWMutex

	plugin.UnimplementedSource
}

var _ plugin.Client = (*Client)(nil)

// New creates a new Apache Arrow destination client
func New(ctx context.Context, logger zerolog.Logger, specBytes []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger:  logger.With().Str("module", "arrowflight").Logger(),
		writers: make(map[string]*flight.Writer),
	}
	if opts.NoConnection {
		return c, nil
	}

	if err := json.Unmarshal(specBytes, &c.spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal spec: %w", err)
	}

	var err error
	if c.flightClient, err = flight.NewClientWithMiddlewareCtx(ctx, c.spec.Addr, NewAuthHandler(c.spec.Handshake, c.spec.Token), nil, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
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
			if err := c.Insert(context.Background(), v); err != nil {
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
	for _, flightDoPutClient := range c.flightDoPutClients {
		if err := flightDoPutClient.CloseSend(); err != nil {
			return fmt.Errorf("failed to close send: %w", err)
		}
		flightPutResult, err := flightDoPutClient.Recv()
		if errors.Is(err, io.EOF) {
			continue
		} else if err != nil {
			return fmt.Errorf("failed to receive put result: %w", err)
		}
		c.logger.Info().Str("appMetadata", string(flightPutResult.GetAppMetadata())).Msg("put result")
	}
	for _, writer := range c.writers {
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
	}
	if err := c.flightClient.Close(); err != nil {
		return fmt.Errorf("failed to close flight client: %w", err)
	}
	return nil
}
