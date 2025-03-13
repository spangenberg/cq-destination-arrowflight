package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spangenberg/cq-destination-arrowflight/client/spec"
)

func ConnectionTester(ctx context.Context, _ zerolog.Logger, specBytes []byte) error {
	var s spec.Spec
	if err := json.Unmarshal(specBytes, &s); err != nil {
		return &plugin.TestConnError{
			Code:    "INVALID_SPEC",
			Message: fmt.Errorf("failed to unmarshal spec: %w", err),
		}
	}
	s.SetDefaults()
	if err := s.Validate(); err != nil {
		return &plugin.TestConnError{
			Code:    "INVALID_SPEC",
			Message: fmt.Errorf("failed to validate spec: %w", err),
		}
	}

	var transportCredentials credentials.TransportCredentials
	if s.TlsEnabled {
		transportCredentials = credentials.NewTLS(&tls.Config{
			ServerName:         s.TlsServerName,
			InsecureSkipVerify: s.TlsInsecureSkipVerify,
		})
	} else {
		transportCredentials = insecure.NewCredentials()
	}
	grpcDialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(s.MaxCallRecvMsgSize),
			grpc.MaxCallSendMsgSize(s.MaxCallSendMsgSize),
		),
	}

	c, err := flight.NewClientWithMiddlewareCtx(ctx, s.Addr, newAuthHandler(s.Handshake, s.Token), nil, grpcDialOptions...)
	if err != nil {
		return err
	}
	defer func(c flight.Client) {
		_ = c.Close()
	}(c)

	return nil
}
