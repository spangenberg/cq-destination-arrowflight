package client

import (
	"time"

	"github.com/cloudquery/plugin-sdk/v4/configtype"
)

const (
	defaultBatchSize      = 10000
	defaultBatchSizeBytes = 100000000
	defaultBatchTimeout   = 60 * time.Second
)

// Spec is the configuration for the Arrow Flight client.
type Spec struct {
	Addr           string              `json:"addr,omitempty"`
	Handshake      string              `json:"handshake,omitempty"`
	Token          string              `json:"token,omitempty"`
	BatchSize      int                 `json:"batch_size,omitempty"`
	BatchSizeBytes int                 `json:"batch_size_bytes,omitempty"`
	BatchTimeout   configtype.Duration `json:"batch_timeout,omitempty"`
}

// SetDefaults sets the default values for the spec.
func (s *Spec) SetDefaults() {
	if s.BatchSize <= 0 {
		s.BatchSize = defaultBatchSize
	}
	if s.BatchSizeBytes <= 0 {
		s.BatchSizeBytes = defaultBatchSizeBytes
	}
	if s.BatchTimeout.Duration() <= 0 {
		s.BatchTimeout = configtype.NewDuration(defaultBatchTimeout)
	}
}
