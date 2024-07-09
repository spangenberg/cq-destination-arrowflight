package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Client) doPutTelemetry(flightDoPutClient flight.FlightService_DoPutClient) {
	for {
		flightPutResult, err := flightDoPutClient.Recv()
		if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
			return
		} else if err != nil {
			c.logger.Error().Err(err).Msg("failed to receive put result")
			return
		}
		c.logger.Info().Str("appMetadata", string(flightPutResult.GetAppMetadata())).Msg("put result")
	}
}

func (c *Client) InsertBatch(ctx context.Context, messages message.WriteInserts) error {
	for _, msg := range messages {
		if err := c.Insert(ctx, msg); err != nil {
			return fmt.Errorf("failed to insert: %w", err)
		}
	}
	return nil
}

func (c *Client) Insert(ctx context.Context, msg *message.WriteInsert) error {
	writer, err := c.insertWriter(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to get writer: %w", err)
	}
	if err = writer.Write(msg.Record); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}
	return nil
}

func (c *Client) insertWriter(ctx context.Context, msg *message.WriteInsert) (*flight.Writer, error) {
	table := msg.GetTable()
	c.lock.RLock()
	writer, ok := c.writers[table.Name]
	c.lock.RUnlock()
	if ok {
		return writer, nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	flightDoPutClient, err := c.flightClient.DoPut(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create do put client: %w", err)
	}
	go c.doPutTelemetry(flightDoPutClient)
	c.flightDoPutClients = append(c.flightDoPutClients, flightDoPutClient)
	writer = flight.NewRecordWriter(flightDoPutClient, ipc.WithSchema(msg.Record.Schema()))
	writer.SetFlightDescriptor(flightDescriptor(table.Name))
	c.writers[table.Name] = writer
	return writer, nil
}
