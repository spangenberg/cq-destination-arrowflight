package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v14/arrow/flight"
)

func (c *Client) doAction(ctx context.Context, actionType string, body []byte) ([]byte, error) {
	var flightDoActionClient flight.FlightService_DoActionClient
	{
		var err error
		if flightDoActionClient, err = c.flightClient.DoAction(ctx, &flight.Action{
			Type: actionType,
			Body: body,
		}); err != nil {
			return nil, fmt.Errorf("failed to create doAction client: %w", err)
		}
	}
	var result *flight.Result
	{
		var err error
		if result, err = flightDoActionClient.Recv(); errors.Is(err, io.EOF) {
			return nil, nil
		} else if err != nil {
			return nil, fmt.Errorf("failed to receive doAction result: %w", err)
		}
	}
	return result.GetBody(), nil
}
