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
    # close_timeout: 1s
    # max_call_recv_msg_size: 10485760 # 10MB
    # max_call_send_msg_size: 10485760 # 10MB
    # tls_enabled: false
    # tls_server_name: ""
    # tls_insecure_skip_verify: false
```
