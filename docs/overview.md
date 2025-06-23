---
name: ArrowFlight
stage: GA
title: Apache Arrow Flight Destination Plugin
description: CloudQuery Apache Arrow Flight destination plugin documentation
---

# ArrowFlight Destination Plugin

:badge

This destination plugin lets you sync data from a CloudQuery source to an Apache Arrow Flight compatible service.

## Configuration

### Example

:configuration

The (top level) spec section is described in the [Destination Spec Reference](https://www.cloudquery.io/docs/reference/destination-spec).

:::callout{type="info"}
Make sure you use environment variable expansion in production instead of committing the credentials to the configuration file directly.
:::

### ArrowFlight Spec

This is the (nested) spec used by the ArrowFlight destination Plugin.

- `addr` (`string`) (required)

  The address of the ArrowFlight service.

    - `localhost:9090` _connect to localhost on port 9090_

- `handshake` (`string`) (optional)

  This parameter is used to authenticate with the ArrowFlight service during the handshake.

- `token` (`string`) (optional)

  This parameter is used to subsequently authenticate with the ArrowFlight service in future calls.
  This parameter will be overridden by the response from the Handshake if the `handshake` parameter is specified.

- `max_call_recv_msg_size` (`integer`) (optional) (default: `4194304` (= 4MB))

  This parameter is used to set the maximum message size in bytes the client can receive.
  If this is not set, gRPC uses the default.

- `max_call_send_msg_size` (`integer`) (optional) (default: `2147483647` (= 2GB))

  This parameter is used to set the maximum message size in bytes the client can send.
  If this is not set, gRPC uses the default.

- `tls_enabled` (`boolean`) (optional) (default: `false`)

  This parameter is used to Enable TLS.

- `tls_server_name` (`string`) (optional)

  This parameter is used to set the server name used to verify the hostname on the returned certificates.

- `tls_insecure_skip_verify` (`boolean`) (optional) (default: `false`)

  This parameter is used to skip the verification of the server's certificate chain and host name.
