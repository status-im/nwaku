## HTTP REST API

The HTTP REST API consists of a set of methods operating on the Waku Node remotely over HTTP.

This API is divided in different _namespaces_ which group a set of resources:

| Namespace | Description |
------------|--------------
| `/debug` | Information about a Waku v2 node. |
| `/relay` | Control of the relaying of messages. See [11/WAKU2-RELAY](https://rfc.vac.dev/spec/11/) RFC |
| `/store` | Retrieve the message history. See [13/WAKU2-STORE](https://rfc.vac.dev/spec/13/) RFC |
| `/filter` | Control of the content filtering. See [12/WAKU2-FILTER](https://rfc.vac.dev/spec/12/) RFC |
| `/admin` | Privileged access to the internal operations of the node. |
| `/private` | Provides functionality to encrypt/decrypt `WakuMessage` payloads using either symmetric or asymmetric cryptography. This allows backwards compatibility with Waku v1 nodes. |


### API Specification

The HTTP REST API has been designed following the OpenAPI 3.0.3 standard specification format.
The OpenAPI specification files can be found in the [Waku Node REST API Reference](https://waku-org.github.io/waku-rest-api/) repository.

You can also use [hosted OpenAPI UI](https://waku-org.github.io/waku-rest-api/) to explore and execute the calls locally.

Check the [OpenAPI Tools](https://openapi.tools/) site for the right tool for you (e.g. REST API client generator)

A particular OpenAPI spec can be easily imported into [Postman](https://www.postman.com/downloads/)
  1. Open Postman.
  2. Click on File -> Import...
  2. Load the openapi.yaml of interest, stored in your computer.
  3. Then, requests can be made from within the 'Collections' section.


### Usage example

#### [`get_waku_v2_debug_v1_info`](https://rfc.vac.dev/spec/16/#get_waku_v2_debug_v1_info)

```bash
curl http://localhost:8645/debug/v1/info -s | jq
```

#### [`get_waku_v2_store_v3_messages`](https://rfc.vac.dev/spec/16/#get_waku_v2_store_v3_messages)

```bash
curl -v -X GET "http://127.0.0.1:49153/store/v3/messages?includeData=true&pubsubTopic=/waku/2/rs/3/0&pageSize=20&ascending=true"
```

or call it encoded

```bash
curl -v -X GET "http://127.0.0.1:5213/store/v3/messages?includeData=true&pubsubTopic=%2Fwaku%2F2%2Frs%2F3%2F0&pageSize=20&ascending=true"
```

In both cases, it works and retrieves the message with the correct topic name.

### Node configuration
Find details [here](https://github.com/waku-org/nwaku/tree/master/docs/operators/how-to/configure-rest-api.md)
