package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

const (
	deleteStale       = "DeleteStale"
	forceMigrateTable = "ForceMigrateTable"
	migrateTable      = "MigrateTable"
)

// MigrateTableBatch is called when a table is created or updated
func (c *Client) MigrateTableBatch(ctx context.Context, messages message.WriteMigrateTables) error {
	for _, msg := range messages {
		table := msg.GetTable()
		c.logger.Debug().Str("tableName", table.Name).Bool("forceMigrate", msg.MigrateForce).Msg("migrate table")

		actionType := migrateTable
		if msg.MigrateForce {
			actionType = forceMigrateTable
		}

		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(table.ToArrowSchema()))
		if err := w.Close(); err != nil {
			c.logger.Warn().Err(err).Msg("failed to close writer")
			continue
		}

		body, err := c.doAction(ctx, actionType, buf.Bytes())
		if err != nil {
			c.logger.Warn().Err(err).Msg("failed to doAction")
			continue
		}
		c.logger.Debug().Str("body", string(body)).Msg("doAction result")
	}
	return nil
}

// InsertBatch is called when a record is inserted
func (c *Client) InsertBatch(ctx context.Context, messages message.WriteInserts) error {
	writers := make(map[string]*flight.Writer)
	var clients []flight.FlightService_DoPutClient

	for _, msg := range messages {
		rec := msg.Record
		md := rec.Schema().Metadata()
		tableName, ok := md.GetValue(schema.MetadataTableName)
		if !ok {
			c.logger.Error().Msg("table name not found in metadata")
			continue
		}
		writer, ok := writers[tableName]
		if !ok {
			var flightDoPutClient flight.FlightService_DoPutClient
			{
				var err error
				if flightDoPutClient, err = c.flightClient.DoPut(ctx); err != nil {
					c.logger.Error().Err(err).Msg("failed to create doPut client")
					continue
				}
			}
			clients = append(clients, flightDoPutClient)
			writer = flight.NewRecordWriter(flightDoPutClient,
				ipc.WithSchema(rec.Schema()))
			defer func(writer *flight.Writer) {
				if err := writer.Close(); err != nil {
					c.logger.Warn().Err(err).Msg("failed to close writer")
				}
			}(writer)

			writer.SetFlightDescriptor(&flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{"cloudquery", "apachearrow", tableName},
			})
			writers[tableName] = writer
		}

		if err := writer.Write(rec); err != nil {
			c.logger.Error().Err(err).Msg("failed to write record")
			continue
		}
	}
	for _, flightDoPutClient := range clients {
		if err := flightDoPutClient.CloseSend(); err != nil {
			return fmt.Errorf("failed to close send: %w", err)
		}
		var flightPutResult *flight.PutResult
		{
			var err error
			if flightPutResult, err = flightDoPutClient.Recv(); errors.Is(err, io.EOF) {
				continue
			} else if err != nil {
				c.logger.Warn().Err(err).Msg("failed to receive put result")
				continue
			}
		}
		c.logger.Info().Str("appMetadata", string(flightPutResult.GetAppMetadata())).Msg("put result")
	}
	return nil
}

// DeleteStaleBatch is called when a record is deleted
func (c *Client) DeleteStaleBatch(ctx context.Context, messages message.WriteDeleteStales) error {
	for _, msg := range messages {
		table := msg.GetTable()
		c.logger.Debug().Str("tableName", table.Name).Msg("delete stale")
		m := struct {
			TableName  string    `json:"tableName"`
			SourceName string    `json:"sourceName"`
			SyncTime   time.Time `json:"syncTime"`
		}{
			TableName:  table.Name,
			SourceName: msg.SourceName,
			SyncTime:   msg.SyncTime,
		}
		var byt []byte
		{
			var err error
			// TODO: use proto instead of json
			if byt, err = json.Marshal(m); err != nil {
				c.logger.Warn().Err(err).Msg("failed to marshal")
				continue
			}
		}

		var body []byte
		{
			var err error
			if body, err = c.doAction(ctx, deleteStale, byt); err != nil {
				c.logger.Warn().Err(err).Msg("failed to doAction")
				continue
			}
		}
		c.logger.Debug().Str("body", string(body)).Msg("doAction result")
	}
	return nil
}

// DeleteRecordsBatch is called when a record is deleted
func (c *Client) DeleteRecordsBatch(ctx context.Context, messages message.WriteDeleteRecords) error {
	for _, msg := range messages {
		table := msg.GetTable()
		tableRelations := msg.TableRelations
		for _, tableRelation := range tableRelations {
			c.logger.Debug().Str("tableName", tableRelation.TableName).Str("parentTable", tableRelation.ParentTable).Msg("delete records")
		}
		whereClause := msg.WhereClause
		for _, predicateGroup := range whereClause {
			c.logger.Debug().Str("groupingType", predicateGroup.GroupingType).Msg("delete records")
			predicates := predicateGroup.Predicates
			for _, predicate := range predicates {
				c.logger.Debug().Str("operator", predicate.Operator).Str("column", predicate.Column).Any("record", predicate.Record).Msg("delete records")
			}
		}
		syncTime := msg.SyncTime
		c.logger.Debug().Time("syncTime", syncTime).Msg("delete records")
		c.logger.Debug().Str("tableName", table.Name).Msg("delete records")
		c.logger.Warn().Msg("delete records not implemented")
	}
	return nil
}
