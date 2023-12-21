package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
)

func (c *Client) MigrateTable(ctx context.Context, msgs <-chan *message.WriteMigrateTable) error {
	for msg := range msgs {
		if err := c.migrateTable(ctx, msg); err != nil {
			return fmt.Errorf("failed to migrate table: %w", err)
		}
	}
	return nil
}

func (c *Client) DeleteStale(ctx context.Context, msgs <-chan *message.WriteDeleteStale) error {
	for msg := range msgs {
		if err := c.deleteStale(ctx, msg); err != nil {
			return fmt.Errorf("failed to delete stale: %w", err)
		}
	}
	return nil
}

func (c *Client) DeleteRecords(ctx context.Context, msgs <-chan *message.WriteDeleteRecord) error {
	for msg := range msgs {
		if err := c.deleteRecord(ctx, msg); err != nil {
			return fmt.Errorf("failed to delete record: %w", err)
		}
	}
	return nil
}

func (c *Client) WriteTable(ctx context.Context, msgs <-chan *message.WriteInsert) error {
	c.logger.Info().Msg("WriteTable")
	var flightDoPutClients []flight.FlightService_DoPutClient
	var writers map[string]*flight.Writer

	for msg := range msgs {
		table := msg.GetTable()
		writer, ok := writers[table.Name]
		if !ok {
			flightDoPutClient, err := c.flightClient.DoPut(ctx)
			if err != nil {
				return fmt.Errorf("failed to create do put client: %w", err)
			}
			flightDoPutClients = append(flightDoPutClients, flightDoPutClient)
			writer = flight.NewRecordWriter(flightDoPutClient, ipc.WithSchema(msg.Record.Schema()))
			writer.SetFlightDescriptor(flightDescriptor(table.Name))
			writers[table.Name] = writer
		}
		if err := writer.Write(msg.Record); err != nil {
			return fmt.Errorf("failed to write: %w", err)
		}
		return nil
	}
	for _, writer := range writers {
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
	}
	for _, flightDoPutClient := range flightDoPutClients {
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
	return nil
}
