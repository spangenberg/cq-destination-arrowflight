package client

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/memory"
	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"google.golang.org/protobuf/proto"
)

const (
	migrateTable = "MigrateTable"
)

func (c *Client) MigrateTableBatch(ctx context.Context, messages message.WriteMigrateTables) error {
	for _, msg := range messages {
		if err := c.MigrateTable(ctx, msg); err != nil {
			return fmt.Errorf("failed to migrate table: %w", err)
		}
	}
	return nil
}

// MigrateTable is called when a table is created or updated
func (c *Client) MigrateTable(ctx context.Context, msg *message.WriteMigrateTable) error {
	table := msg.GetTable()
	c.logger.Debug().Str("tableName", table.Name).Bool("forceMigrate", msg.MigrateForce).Msg("migrate table")
	data, err := proto.Marshal(&pb.Write_MessageMigrateTable{
		Table:        flight.SerializeSchema(table.ToArrowSchema(), memory.DefaultAllocator),
		MigrateForce: msg.MigrateForce,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}
	var body []byte
	if body, err = c.doAction(ctx, migrateTable, data); err != nil {
		return fmt.Errorf("failed to doAction: %w", err)
	}
	c.logger.Debug().Str("body", string(body)).Msg("migrate table result")
	return nil
}
