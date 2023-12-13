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

The (top level) spec section is described in the [Destination Spec Reference](/docs/reference/destination-spec).

:::callout{type="info"}
Make sure you use environment variable expansion in production instead of committing the credentials to the configuration file directly.
:::

The ArrowFlight destination utilizes batching, and supports [`batch_size`](/docs/reference/destination-spec#batch_size) and [`batch_size_bytes`](/docs/reference/destination-spec#batch_size_bytes).

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

- `batch_size` (`integer`) (optional) (default: `10000`)

  This parameter controls the maximum amount of items may be grouped together to be written as a single write.

- `batch_size_bytes` (`integer`) (optional) (default: `100000000` (= 100 MB))

  This parameter controls the maximum size of items that may be grouped together to be written as a single write.

- `batch_timeout` (`duration`) (optional) (default: `60s` (= 60 seconds))

  This parameter controls the timeout for writing a single batch.
