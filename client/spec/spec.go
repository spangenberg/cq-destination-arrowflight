package spec

import (
	"errors"
)

const (
	defaultMaxCallRecvMsgSize = 4000000
	defaultMaxCallSendMsgSize = 2147483647
)

type Spec struct {
	// The address of the ArrowFlight service.
	Addr string `json:"addr,omitempty" jsonschema:"required,minLength=1,example=localhost:9090"`

	// This parameter is used to authenticate with the ArrowFlight service during the handshake.
	Handshake string `json:"handshake,omitempty"`

	// This parameter is used to subsequently authenticate with the ArrowFlight service in future calls.
	// This parameter will be overridden by the response from the Handshake if the `handshake` parameter is specified.
	Token string `json:"token,omitempty"`

	// This parameter is used to set the maximum message size in bytes the client can send.
	//  If this is not set, gRPC uses the default.
	MaxCallRecvMsgSize int `json:"max_call_recv_msg_size,omitempty" jsonschema:"minimum=1,default=4000000"`
	MaxCallSendMsgSize int `json:"max_call_send_msg_size,omitempty" jsonschema:"minimum=1,default=2147483647"`

	// This parameter is used to Enable TLS.
	TlsEnabled bool `json:"tls_enabled,omitempty"`

	// This parameter is used to set the server name used to verify the hostname on the returned certificates.
	TlsServerName string `json:"tls_server_name,omitempty"`

	// This parameter is used to skip the verification of the server's certificate chain and host name.
	TlsInsecureSkipVerify bool `json:"tls_insecure_skip_verify,omitempty"`
}

func (s *Spec) SetDefaults() {
	if s.MaxCallRecvMsgSize <= 0 {
		s.MaxCallRecvMsgSize = defaultMaxCallRecvMsgSize
	}
	if s.MaxCallSendMsgSize <= 0 {
		s.MaxCallSendMsgSize = defaultMaxCallSendMsgSize
	}
}

func (s *Spec) Validate() error {
	if len(s.Addr) == 0 {
		return errors.New("`addr` is required")
	}

	return nil
}
