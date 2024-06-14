package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
)

func flightDescriptor(tableName string) *flight.FlightDescriptor {
	return &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"cloudquery", "arrowflight", tableName},
	}
}

func (c *Client) doAction(ctx context.Context, actionType string, body []byte) ([]byte, error) {
	flightDoActionClient, err := c.flightClient.DoAction(ctx, &flight.Action{
		Type: actionType,
		Body: body,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create doAction client: %w", err)
	}
	var result *flight.Result
	if result, err = flightDoActionClient.Recv(); errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to receive doAction result: %w", err)
	}
	return result.GetBody(), nil
}

func (c *Client) doGet(ctx context.Context, endpoint *flight.FlightEndpoint, schema *arrow.Schema, res chan<- arrow.Record) error {
	doGetClient, err := c.flightClient.DoGet(ctx, endpoint.GetTicket())
	if err != nil {
		return fmt.Errorf("failed to create doPut client: %w", err)
	}
	var recordReader *flight.Reader
	if recordReader, err = flight.NewRecordReader(doGetClient, ipc.WithSchema(schema)); err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer recordReader.Release()
	for recordReader.Next() {
		r := recordReader.Record()
		r.Retain()
		res <- r
	}
	if err = recordReader.Err(); err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}
	return nil
}

func (c *Client) getFlightInfo(ctx context.Context, tableName string) (*flight.FlightInfo, error) {
	flightInfo, err := c.flightClient.GetFlightInfo(ctx, flightDescriptor(tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get flight info: %w", err)
	}
	return flightInfo, nil
}
