package client

import (
	"context"
	"fmt"

	"github.com/cloudquery/plugin-sdk/v4/message"
)

func (c *Client) Insert(ctx context.Context, msg *message.WriteInsert) error {
	writer, ok := c.getWriter(msg)
	if !ok {
		if err := c.createWriter(ctx, msg); err != nil {
			return fmt.Errorf("create table writer failed: %w", err)
		}
		if writer, ok = c.getWriter(msg); !ok {
			return fmt.Errorf("table writer not found after creation")
		}
	}

	if err := writer.Write(ctx, msg); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	return nil
}
