package main

import (
	"context"
	"log"

	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/serve"

	"github.com/spangenberg/cq-destination-arrowflight/client"
	internalPlugin "github.com/spangenberg/cq-destination-arrowflight/resources/plugin"
)

func main() {
	p := plugin.NewPlugin(
		internalPlugin.Name,
		internalPlugin.Version,
		client.New,
		plugin.WithKind(internalPlugin.Kind),
		plugin.WithTeam(internalPlugin.Team),
	)
	if err := serve.Plugin(p).Serve(context.Background()); err != nil {
		log.Fatalf("failed to serve plugin: %v", err)
	}
}
