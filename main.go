package main

import (
	"context"
	"log"

	pluginSDK "github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/serve"

	"github.com/spangenberg/cq-destination-arrowflight/client"
	"github.com/spangenberg/cq-destination-arrowflight/client/spec"
	"github.com/spangenberg/cq-destination-arrowflight/resources/plugin"
)

func main() {
	p := pluginSDK.NewPlugin(plugin.Name, plugin.Version, client.New,
		pluginSDK.WithKind(plugin.Kind),
		pluginSDK.WithTeam(plugin.Team),
		pluginSDK.WithJSONSchema(spec.JSONSchema),
		pluginSDK.WithConnectionTester(client.ConnectionTester),
	)

	if err := serve.Plugin(p, serve.WithDestinationV0V1Server()).Serve(context.Background()); err != nil {
		log.Fatalf("failed to serve plugin: %v", err)
	}
}
