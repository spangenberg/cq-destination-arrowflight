package client

import (
	"time"

	"github.com/cloudquery/plugin-sdk/v4/configtype"
)

const (
	defaultCloseTimeout = time.Second
)

// Spec is the configuration for the Arrow Flight client.
type Spec struct {
	Addr      string `json:"addr,omitempty"`
	Handshake string `json:"handshake,omitempty"`
	Token     string `json:"token,omitempty"`

	CloseTimeout configtype.Duration `json:"close_timeout,omitempty"`

	MaxCallRecvMsgSize *int `json:"max_call_recv_msg_size,omitempty"`
	MaxCallSendMsgSize *int `json:"max_call_send_msg_size,omitempty"`

	TlsEnabled            bool   `json:"tls_enabled,omitempty"`
	TlsServerName         string `json:"tls_server_name,omitempty"`
	TlsInsecureSkipVerify bool   `json:"tls_insecure_skip_verify,omitempty"`
}

func (s *Spec) SetDefaults() {
	if s.CloseTimeout.Duration() <= 0 {
		s.CloseTimeout = configtype.NewDuration(defaultCloseTimeout)
	}
}
