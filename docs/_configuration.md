This example configures an ArrowFlight destination, located at `localhost:9090`:

```yaml copy
kind: destination
spec:
  name: "arrowflight"
  path: "cloudquery/arrowflight"
  registry: "cloudquery"
  version: "VERSION_DESTINATION_ARROWFLIGHT"
  spec:
    addr: "localhost:9090"
    # Optional parameters:
    # handshake: ""
    # token: ""
    # batch_size: 10000 # 10K entries
    # batch_size_bytes: 100000000 # 100 MB
    # batch_timeout: 60s
```
