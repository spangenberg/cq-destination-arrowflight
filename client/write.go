package client

import (
	"context"
	"fmt"

	"github.com/cloudquery/plugin-sdk/v4/message"
)

func (c *Client) Write(ctx context.Context, res <-chan message.WriteMessage) error {
	for r := range res {
		switch m := r.(type) {
		case *message.WriteMigrateTable:
			if err := c.MigrateTable(ctx, m); err != nil {
				return fmt.Errorf("failed to migrate table %s: %w", m.Table.Name, err)
			}
		case *message.WriteInsert:
			if err := c.Insert(ctx, m); err != nil {
				return fmt.Errorf("failed to insert record: %w", err)
			}
		case *message.WriteDeleteStale:
			if err := c.DeleteStale(ctx, m); err != nil {
				return fmt.Errorf("failed to delete stale records: %w", err)
			}
		case *message.WriteDeleteRecord:
			if err := c.DeleteRecord(ctx, m); err != nil {
				return fmt.Errorf("failed to delete record: %w", err)
			}
		default:
			return fmt.Errorf("unhandled message type: %T", m)
		}
	}

	return nil
}
