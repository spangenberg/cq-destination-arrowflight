package client

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow/flight"
)

// AuthHandler is a simple auth handler that sends a token and reads a token
type AuthHandler struct {
	handshake []byte
	token     string
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(handshake, token string) *AuthHandler {
	if handshake == "" {
		return nil
	}
	return &AuthHandler{
		handshake: []byte(handshake),
		token:     token,
	}
}

// Authenticate sends the auth token and reads the token
func (c *AuthHandler) Authenticate(_ context.Context, conn flight.AuthConn) error {
	if err := conn.Send(c.handshake); err != nil {
		return fmt.Errorf("failed to send auth request: %w", err)
	}
	byt, err := conn.Read()
	if err != nil {
		return fmt.Errorf("failed to read auth request: %w", err)
	}
	c.token = string(byt)
	return nil
}

// GetToken returns the token
func (c *AuthHandler) GetToken(context.Context) (string, error) {
	if c == nil {
		return "", nil
	}
	return c.token, nil
}
