# Configure a REST API node

A subset of the node configuration can be used to modify the behaviour of the HTTP REST API.

These are the relevant command line options:

| CLI option | Description | Default value |
|------------|-------------|---------------|
|`--rest` | Enable Waku REST HTTP server. | `false` |
|`--rest-address` | Listening address of the REST HTTP server. | `127.0.0.1` |
|`--rest-port` | Listening port of the REST HTTP server. | `8645` |
|`--rest-relay-cache-capacity` | Capacity of the Relay REST API message cache. | `30` |
|`--rest-admin` | Enable access to REST HTTP Admin API. | `false` |
|`--rest-private` | Enable access to REST HTTP Private API. | `false` |

Note that these command line options have their counterpart option in the node configuration file.

Example:

```shell
wakunode2 --rest=true
```

The `page_size` flag in the Store API has a default value of 20 and a max value of 100.
