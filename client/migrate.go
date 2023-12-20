package client

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/memory"
	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/golang/protobuf/proto"
)

const (
	migrateTable = "MigrateTable"
)

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
