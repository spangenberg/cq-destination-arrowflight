package client

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionTester(t *testing.T) {
	type wantErr struct {
		Code             string
		ErrorDescription string
	}

	tests := []struct {
		name      string
		specBytes []byte
		wantErr   *wantErr
	}{
		{
			name:      "should return an error for an invalid spec",
			specBytes: []byte("invalid"),
			wantErr: &wantErr{
				Code:             "INVALID_SPEC",
				ErrorDescription: "failed to unmarshal spec: invalid character 'i' looking for beginning of value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := plugin.NewPlugin(
				"arrowflight",
				"development",
				New,
				plugin.WithConnectionTester(ConnectionTester),
			)

			logger := zerolog.New(os.Stdout)

			err := p.TestConnection(context.Background(), logger, tt.specBytes)
			if tt.wantErr != nil {
				require.Error(t, err)
				var target *plugin.TestConnError
				if errors.As(err, &target) {
					assert.Equal(t, tt.wantErr.Code, target.Code)
					assert.Equal(t, tt.wantErr.ErrorDescription, target.Message.Error())
					return
				}
				assert.Equal(t, tt.wantErr.ErrorDescription, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
