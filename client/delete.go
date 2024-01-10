package client

import (
	"context"
	"fmt"

	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	deleteStale  = "DeleteStale"
	deleteRecord = "DeleteRecord"
)

// DeleteStale is called when a record is deleted
func (c *Client) DeleteStale(ctx context.Context, msg *message.WriteDeleteStale) error {
	table := msg.GetTable()
	c.logger.Debug().Str("tableName", table.Name).Str("sourceName", msg.SourceName).Time("syncTime", msg.SyncTime).Msg("delete stale")
	data, err := proto.Marshal(&pb.Write_MessageDeleteStale{
		SourceName: msg.SourceName,
		SyncTime:   timestamppb.New(msg.SyncTime),
		TableName:  msg.TableName,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}
	var body []byte
	if body, err = c.doAction(ctx, deleteStale, data); err != nil {
		return fmt.Errorf("failed to doAction: %w", err)
	}
	c.logger.Debug().Str("body", string(body)).Msg("delete stale result")
	return nil
}

// DeleteRecord is called when a record is deleted
func (c *Client) DeleteRecord(ctx context.Context, msg *message.WriteDeleteRecord) error {
	table := msg.GetTable()
	c.logger.Debug().Str("tableName", table.Name).Msg("delete records")
	var whereClause []*pb.PredicatesGroup
	for _, predicateGroup := range msg.WhereClause {
		var predicates []*pb.Predicate
		for _, predicate := range predicateGroup.Predicates {
			record, err := pb.RecordToBytes(predicate.Record)
			if err != nil {
				return fmt.Errorf("failed to convert record to bytes: %w", err)
			}
			operator := pb.Predicate_Operator(pb.Predicate_Operator_value[predicate.Operator])
			predicates = append(predicates, &pb.Predicate{
				Operator: operator,
				Column:   predicate.Column,
				Record:   record,
			})
		}
		groupingType := pb.PredicatesGroup_GroupingType(pb.PredicatesGroup_GroupingType_value[predicateGroup.GroupingType])
		whereClause = append(whereClause, &pb.PredicatesGroup{
			GroupingType: groupingType,
			Predicates:   predicates,
		})
	}
	var tableRelations []*pb.TableRelation
	for _, tableRelation := range msg.TableRelations {
		tableRelations = append(tableRelations, &pb.TableRelation{
			TableName:   tableRelation.TableName,
			ParentTable: tableRelation.ParentTable,
		})
	}
	data, err := proto.Marshal(&pb.Write_MessageDeleteRecord{
		TableName:      table.Name,
		WhereClause:    whereClause,
		TableRelations: tableRelations,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}
	var body []byte
	if body, err = c.doAction(ctx, deleteRecord, data); err != nil {
		return fmt.Errorf("failed to doAction: %w", err)
	}
	c.logger.Debug().Str("body", string(body)).Msg("delete records result")
	return nil
}
