package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/writers/streamingbatchwriter"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the client for the Apache Arrow destination
type Client struct {
	logger zerolog.Logger
	spec   Spec
	writer *streamingbatchwriter.StreamingBatchWriter

	flightClient flight.Client

	plugin.UnimplementedSource
}

var _ plugin.Client = (*Client)(nil)

// New creates a new Apache Arrow destination client
func New(ctx context.Context, logger zerolog.Logger, specBytes []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger: logger.With().Str("module", "arrowflight").Logger(),
	}
	if opts.NoConnection {
		return c, nil
	}

	if err := json.Unmarshal(specBytes, &c.spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal spec: %w", err)
	}

	var err error
	c.writer, err = streamingbatchwriter.New(c,
		streamingbatchwriter.WithLogger(c.logger),
		//streamingbatchwriter.WithBatchSizeRows(spec.BatchSizeRows),
		//streamingbatchwriter.WithBatchSizeBytes(spec.BatchSizeBytes),
		//streamingbatchwriter.WithBatchTimeout(spec.BatchTimeout.Duration()),
	)
	if err != nil {
		return nil, err
	}

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
func (c *Client) Write(ctx context.Context, msgs <-chan message.WriteMessage) error {
	return c.writer.Write(ctx, msgs)
}

// Close is called when the client is closed
func (c *Client) Close(ctx context.Context) error {
	if err := c.writer.Close(ctx); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	if err := c.flightClient.Close(); err != nil {
		return fmt.Errorf("failed to close flight client: %w", err)
	}
	return nil
}
