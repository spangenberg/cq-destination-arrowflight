package client

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"

	"github.com/spangenberg/cq-destination-arrowflight/client/spec"
)

func getTestAddr() string {
	testAddr := os.Getenv("CQ_DEST_ARROWFLIGHT_TEST_ADDR")
	if testAddr == "" {
		return "localhost:9090"
	}
	return testAddr
}

var safeMigrations = plugin.SafeMigrations{
	AddColumn:              true,
	AddColumnNotNull:       true,
	RemoveColumn:           true,
	RemoveColumnNotNull:    true,
	RemoveUniqueConstraint: true,
	MovePKToCQOnly:         true,
}

func TestPgPlugin(t *testing.T) {
	ctx := context.Background()
	p := plugin.NewPlugin("postgresql", "development", New)
	s := &spec.Spec{
		Addr: getTestAddr(),
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Init(ctx, b, plugin.NewClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	testOpts := schema.TestSourceOptions{
		TimePrecision: time.Nanosecond,
	}
	plugin.TestWriterSuiteRunner(t,
		p,
		plugin.WriterTestSuiteTests{
			SafeMigrations: safeMigrations,
		},
		plugin.WithTestDataOptions(testOpts),
	)
}
