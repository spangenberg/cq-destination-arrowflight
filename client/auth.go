package client

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

type authHandler struct {
	handshake []byte
	token     []byte
}

func newAuthHandler(handshake, token string) *authHandler {
	return &authHandler{
		handshake: []byte(handshake),
		token:     []byte(token),
	}
}

func (c *authHandler) Authenticate(_ context.Context, conn flight.AuthConn) error {
	if err := conn.Send(c.handshake); err != nil {
		return fmt.Errorf("failed to send auth request: %w", err)
	}
	var err error
	if c.token, err = conn.Read(); err != nil {
		return fmt.Errorf("failed to read auth request: %w", err)
	}
	return nil
}

func (c *authHandler) GetToken(context.Context) (string, error) {
	return string(c.token), nil
}
