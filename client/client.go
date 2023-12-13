package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/writers/mixedbatchwriter"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the client for the Apache Arrow destination
type Client struct {
	logger zerolog.Logger
	spec   Spec
	writer *mixedbatchwriter.MixedBatchWriter

	flightClient flight.Client

	plugin.UnimplementedSource
}

var _ plugin.Client = (*Client)(nil)

// New creates a new Apache Arrow destination client
func New(ctx context.Context, logger zerolog.Logger, specBytes []byte, _ plugin.NewClientOptions) (plugin.Client, error) {
	c := &Client{
		logger: logger.With().Str("module", "apachearrow").Logger(),
	}

	if err := json.Unmarshal(specBytes, &c.spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal spec: %w", err)
	}
	c.spec.SetDefaults()

	{
		var err error
		if c.writer, err = mixedbatchwriter.New(c,
			mixedbatchwriter.WithLogger(c.logger),
			mixedbatchwriter.WithBatchSize(c.spec.BatchSize),
			mixedbatchwriter.WithBatchSizeBytes(c.spec.BatchSizeBytes),
			mixedbatchwriter.WithBatchTimeout(c.spec.BatchTimeout.Duration()),
		); err != nil {
			return nil, err
		}
	}

	{
		var err error
		if c.flightClient, err = flight.NewClientWithMiddlewareCtx(ctx, c.spec.Addr, NewAuthHandler(c.spec.Handshake, c.spec.Token), nil, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
			return nil, fmt.Errorf("failed to create flight client: %w", err)
		}
		if c.spec.Handshake != "" {
			if err = c.flightClient.Authenticate(ctx); err != nil {
				return nil, fmt.Errorf("failed to authenticate flight client: %w", err)
			}
		}
	}

	return c, nil
}

// Read is called when a table is read
func (c *Client) Read(_ context.Context, table *schema.Table, _ chan<- arrow.Record) error {
	c.logger.Debug().Str("table", table.Name).Msg("read")
	return fmt.Errorf("not implemented")
}

// Write is called when a table is written
func (c *Client) Write(ctx context.Context, messages <-chan message.WriteMessage) error {
	return c.writer.Write(ctx, messages)
}

// Close is called when the client is closed
func (c *Client) Close(context.Context) error {
	if err := c.flightClient.Close(); err != nil {
		return fmt.Errorf("failed to close flight client: %w", err)
	}
	return nil
}
