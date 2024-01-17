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
    # max_call_recv_msg_size: 10485760 # 10MB
    # max_call_send_msg_size: 10485760 # 10MB
```
