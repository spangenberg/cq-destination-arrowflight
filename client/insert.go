package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Client) doPutTelemetry(ctx context.Context, flightDoPutClient flight.FlightService_DoPutClient) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Debug().Msg("stopping telemetry processing")
			return
		default:
		}

		flightPutResult, err := flightDoPutClient.Recv()
		code := status.Code(err)
		if errors.Is(err, io.EOF) || code == codes.Canceled {
			return
		} else if err != nil {
			if code == codes.Unavailable || code == codes.ResourceExhausted {
				c.logger.Warn().Err(err).Msg("transient error receiving telemetry, will retry")
				time.Sleep(time.Second)
				continue
			}

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
	if writer == nil {
		return c.Insert(ctx, msg)
	}
	if err = writer.Write(msg.Record); errors.Is(err, io.EOF) {
		c.logger.Warn().Err(err).Msg("writer is closed")
		if reconnectErr := c.reconnect(ctx); reconnectErr != nil {
			return errors.Join(err, reconnectErr)
		}
		return c.Insert(ctx, msg)
	} else if err != nil {
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
	c.logger.Info().Str("table", table.Name).Msg("creating do put client")
	flightDoPutClient, err := c.flightClient.DoPut(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create do put client: %w", err)
	}
	go c.doPutTelemetry(ctx, flightDoPutClient)
	c.flightDoPutClients = append(c.flightDoPutClients, flightDoPutClient)
	c.logger.Info().Str("table", table.Name).Msg("creating record writer")
	writer = flight.NewRecordWriter(flightDoPutClient, ipc.WithSchema(msg.Record.Schema()))
	writer.SetFlightDescriptor(flightDescriptor(table.Name))
	c.writers[table.Name] = writer
	return writer, nil
}
