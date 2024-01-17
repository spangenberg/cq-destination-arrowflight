package client

// Spec is the configuration for the Arrow Flight client.
type Spec struct {
	Addr      string `json:"addr,omitempty"`
	Handshake string `json:"handshake,omitempty"`
	Token     string `json:"token,omitempty"`

	MaxCallRecvMsgSize *int `json:"max_call_recv_msg_size,omitempty"`
	MaxCallSendMsgSize *int `json:"max_call_send_msg_size,omitempty"`
}
