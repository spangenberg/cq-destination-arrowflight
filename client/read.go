package client

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

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
