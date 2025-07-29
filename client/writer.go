package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const writeTimeout = 10 * time.Second
const maxRetries = 3

type Writer struct {
	client            *Client
	cancel            context.CancelFunc
	flightDoPutClient flight.FlightService_DoPutClient
	flightWriter      *flight.Writer
	tableName         string
}

func NewWriter(client *Client, tableName string) *Writer {
	return &Writer{
		client:    client,
		tableName: tableName,
	}
}

func (w *Writer) Close() error {
	if w.flightWriter != nil {
		if err := w.flightWriter.Close(); err != nil {
			return fmt.Errorf("failed to close flight writer: %w", err)
		}
	}

	if w.flightDoPutClient != nil {
		if err := w.flightDoPutClient.CloseSend(); err != nil {
			return fmt.Errorf("failed to close flight do put client: %w", err)
		}
	}

	if w.cancel != nil {
		w.cancel()
	}

	return nil
}

func (w *Writer) Write(ctx context.Context, msg *message.WriteInsert) error {
	var size int
	{
		var err error
		if size, err = recordSize(msg.Record); err != nil {
			return fmt.Errorf("failed to calculate record size: %w", err)
		}
	}
	if size > w.client.spec.MaxCallSendMsgSize {
		w.client.logger.Warn().Msg("record size exceeds max call send msg size, skipping write")
		return nil
	}

	return w.writeWithRetries(ctx, msg.Record, 1)
}

func (w *Writer) doPutTelemetry(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.client.logger.Debug().Msg("stopping telemetry processing")
			return
		default:
		}

		flightPutResult, err := w.flightDoPutClient.Recv()
		code := status.Code(err)
		if errors.Is(err, io.EOF) || code == codes.Canceled || code == codes.Unavailable {
			return
		} else if code == codes.ResourceExhausted {
			w.client.logger.Warn().Err(err).Msg("transient error receiving telemetry, will retry")

			time.Sleep(time.Second)

			continue
		} else if err != nil {
			w.client.logger.Error().Err(err).Msg("failed to receive put result")
			return
		}

		w.client.logger.Info().Str("appMetadata", string(flightPutResult.GetAppMetadata())).Msg("put result")
	}
}

func (w *Writer) init(ctx context.Context, rec arrow.Record) error {
	ctx, w.cancel = context.WithCancel(ctx)

	w.client.logger.Info().Str("table", w.tableName).Msg("creating do put client")
	{
		var err error
		if w.flightDoPutClient, err = w.client.flightClient.DoPut(ctx); err != nil {
			w.cancel()

			return fmt.Errorf("failed to create do put client: %w", err)
		}
	}
	go w.doPutTelemetry(ctx)

	w.client.logger.Info().Str("table", w.tableName).Msg("creating record writer")
	w.flightWriter = flight.NewRecordWriter(w.flightDoPutClient, ipc.WithSchema(rec.Schema()))
	w.flightWriter.SetFlightDescriptor(flightDescriptor(w.tableName))

	return nil
}

func (w *Writer) writeWithRetries(ctx context.Context, rec arrow.Record, attempt int) error {
	w.client.logger.Debug().Str("table", w.tableName).Int("attempt", attempt).Msg("writing record")

	if err := w.flightWriter.Write(rec); errors.Is(err, io.EOF) {
		w.cancel()
		if err = w.client.authenticate(ctx); err != nil {
			return fmt.Errorf("failed to reauthenticate after EOF: %w", err)
		}

		// Attempt to reinitialize the writer after EOF
		time.Sleep(time.Duration(attempt) * writeTimeout)

		if reconnectErr := w.init(ctx, rec); reconnectErr != nil {
			if attempt <= maxRetries {
				w.client.logger.Warn().Err(reconnectErr).Int("attempt", attempt).Msg("reinitializing writer after EOF, retrying")

				return w.writeWithRetries(ctx, rec, attempt+1)
			} else {
				w.client.logger.Error().Err(reconnectErr).Msg("failed to reinitialize writer after EOF, giving up")

				return fmt.Errorf("failed to reinitialize writer after EOF: %w", reconnectErr)
			}
		}

		return w.writeWithRetries(ctx, rec, attempt+1)
	} else if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

func (c *Client) createWriter(ctx context.Context, msg *message.WriteInsert) error {
	tableName, found := msg.Record.Schema().Metadata().GetValue(schema.MetadataTableName)
	if !found {
		panic(errors.New("missing table name"))
	}

	writer := NewWriter(c, tableName)
	if err := writer.init(ctx, msg.Record); err != nil {
		return fmt.Errorf("failed to init writer: %w", err)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.writers[tableName] = writer

	return nil
}

func (c *Client) getWriter(msg *message.WriteInsert) (*Writer, bool) {
	tableName, found := msg.Record.Schema().Metadata().GetValue(schema.MetadataTableName)
	if !found {
		panic(errors.New("missing table name"))
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	writer, ok := c.writers[tableName]
	return writer, ok
}

func recordSize(rec arrow.Record) (int, error) {
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	defer func(writer *ipc.Writer) {
		_ = writer.Close()
	}(writer)
	if err := writer.Write(rec); err != nil {
		return -1, fmt.Errorf("failed to write record: %w", err)
	}
	return buf.Len(), nil
}
