package client

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
)

// Insert is called when a record is inserted
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
	c.writersLock.RLock()
	writer, ok := c.writers[table.Name]
	c.writersLock.RUnlock()
	if ok {
		return writer, nil
	}
	c.writersLock.Lock()
	defer c.writersLock.Unlock()
	flightDoPutClient, err := c.flightClient.DoPut(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create do put client: %w", err)
	}
	c.flightDoPutClients = append(c.flightDoPutClients, flightDoPutClient)
	writer = flight.NewRecordWriter(flightDoPutClient, ipc.WithSchema(msg.Record.Schema()))
	writer.SetFlightDescriptor(flightDescriptor(table.Name))
	c.writers[table.Name] = writer
	return writer, nil
}
