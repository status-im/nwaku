{.push raises: [].}

import net, tables
import presto
import
  waku/waku_node,
  waku/discovery/waku_discv5,
  waku/factory/external_config,
  waku/waku_api/message_cache,
  waku/waku_api/handlers,
  waku/waku_api/rest/server,
  waku/waku_api/rest/debug/handlers as rest_debug_api,
  waku/waku_api/rest/relay/handlers as rest_relay_api,
  waku/waku_api/rest/filter/handlers as rest_filter_api,
  waku/waku_api/rest/legacy_lightpush/handlers as rest_legacy_lightpush_api,
  waku/waku_api/rest/lightpush/handlers as rest_lightpush_api,
  waku/waku_api/rest/store/handlers as rest_store_api,
  waku/waku_api/rest/legacy_store/handlers as rest_store_legacy_api,
  waku/waku_api/rest/health/handlers as rest_health_api,
  waku/waku_api/rest/admin/handlers as rest_admin_api,
  waku/waku_core/topics

## Monitoring and external interfaces

# Used to register api endpoints that are not currently installed as keys,
# values are holding error messages to be returned to the client
# NOTE: {.threadvar.} is used to make the global variable GC safe for the closure uses it
# It will always be called from main thread anyway.
# Ref: https://nim-lang.org/docs/manual.html#threads-gc-safety
var restServerNotInstalledTab {.threadvar.}: TableRef[string, string]
restServerNotInstalledTab = newTable[string, string]()

proc startRestServerEsentials*(
    nodeHealthMonitor: WakuNodeHealthMonitor, conf: WakuNodeConf
): Result[WakuRestServerRef, string] =
  if not conf.rest:
    return ok(nil)

  let requestErrorHandler: RestRequestErrorHandler = proc(
      error: RestRequestError, request: HttpRequestRef
  ): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
    try:
      case error
      of RestRequestError.Invalid:
        return await request.respond(Http400, "Invalid request", HttpTable.init())
      of RestRequestError.NotFound:
        let paths = request.rawPath.split("/")
        let rootPath =
          if len(paths) > 1:
            paths[1]
          else:
            ""
        restServerNotInstalledTab[].withValue(rootPath, errMsg):
          return await request.respond(Http404, errMsg[], HttpTable.init())
        do:
          return await request.respond(
            Http400,
            "Bad request initiated. Invalid path or method used.",
            HttpTable.init(),
          )
      of RestRequestError.InvalidContentBody:
        return await request.respond(Http400, "Invalid content body", HttpTable.init())
      of RestRequestError.InvalidContentType:
        return await request.respond(Http400, "Invalid content type", HttpTable.init())
      of RestRequestError.Unexpected:
        return defaultResponse()
    except HttpWriteError:
      error "Failed to write response to client", error = getCurrentExceptionMsg()
      discard

    return defaultResponse()

  let allowedOrigin =
    if len(conf.restAllowOrigin) > 0:
      some(conf.restAllowOrigin.join(","))
    else:
      none(string)

  let address = conf.restAddress
  let port = Port(conf.restPort + conf.portsShift)
  let server =
    ?newRestHttpServer(
      address,
      port,
      allowedOrigin = allowedOrigin,
      requestErrorHandler = requestErrorHandler,
    )

  ## Health REST API
  installHealthApiHandler(server.router, nodeHealthMonitor)

  restServerNotInstalledTab["admin"] =
    "/admin endpoints are not available while initializing."
  restServerNotInstalledTab["debug"] =
    "/debug endpoints are not available while initializing."
  restServerNotInstalledTab["relay"] =
    "/relay endpoints are not available while initializing."
  restServerNotInstalledTab["filter"] =
    "/filter endpoints are not available while initializing."
  restServerNotInstalledTab["lightpush"] =
    "/lightpush endpoints are not available while initializing."
  restServerNotInstalledTab["store"] =
    "/store endpoints are not available while initializing."

  server.start()
  info "Starting REST HTTP server", url = "http://" & $address & ":" & $port & "/"

  ok(server)

proc startRestServerProtocolSupport*(
    restServer: WakuRestServerRef,
    node: WakuNode,
    wakuDiscv5: WakuDiscoveryV5,
    conf: WakuNodeConf,
): Result[void, string] =
  if not conf.rest:
    return ok()

  var router = restServer.router
  ## Admin REST API
  if conf.restAdmin:
    installAdminApiHandlers(router, node)
  else:
    restServerNotInstalledTab["admin"] =
      "/admin endpoints are not available. Please check your configuration: --rest-admin=true"

  ## Debug REST API
  installDebugApiHandlers(router, node)

  ## Relay REST API
  if conf.relay:
    let cache = MessageCache.init(int(conf.restRelayCacheCapacity))

    let handler = messageCacheHandler(cache)

    for shard in conf.shards:
      let pubsubTopic = $RelayShard(clusterId: conf.clusterId, shardId: shard)
      cache.pubsubSubscribe(pubsubTopic)
      node.subscribe((kind: PubsubSub, topic: pubsubTopic), some(handler))

    for contentTopic in conf.contentTopics:
      cache.contentSubscribe(contentTopic)
      node.subscribe((kind: ContentSub, topic: contentTopic), some(handler))

    installRelayApiHandlers(router, node, cache)
  else:
    restServerNotInstalledTab["relay"] =
      "/relay endpoints are not available. Please check your configuration: --relay"

  ## Filter REST API
  if conf.filternode != "" and node.wakuFilterClient != nil:
    let filterCache = MessageCache.init()

    let filterDiscoHandler =
      if not wakuDiscv5.isNil():
        some(defaultDiscoveryHandler(wakuDiscv5, Filter))
      else:
        none(DiscoveryHandler)

    rest_filter_api.installFilterRestApiHandlers(
      router, node, filterCache, filterDiscoHandler
    )
  else:
    restServerNotInstalledTab["filter"] =
      "/filter endpoints are not available. Please check your configuration: --filternode"

  ## Store REST API
  let storeDiscoHandler =
    if not wakuDiscv5.isNil():
      some(defaultDiscoveryHandler(wakuDiscv5, Store))
    else:
      none(DiscoveryHandler)

  rest_store_api.installStoreApiHandlers(router, node, storeDiscoHandler)
  rest_store_legacy_api.installStoreApiHandlers(router, node, storeDiscoHandler)

  ## Light push API
  ## Install it either if lightpushnode (lightpush service node) is configured and client is mounted)
  ## or install it to be used with self-hosted lightpush service
  if (conf.lightpushnode != "" and node.wakuLegacyLightpushClient != nil) or
      (conf.lightpush and node.wakuLegacyLightPush != nil and node.wakuRelay != nil):
    let lightDiscoHandler =
      if not wakuDiscv5.isNil():
        some(defaultDiscoveryHandler(wakuDiscv5, Lightpush))
      else:
        none(DiscoveryHandler)

    rest_legacy_lightpush_api.installLightPushRequestHandler(
      router, node, lightDiscoHandler
    )
    rest_lightpush_api.installLightPushRequestHandler(router, node, lightDiscoHandler)
  else:
    restServerNotInstalledTab["lightpush"] =
      "/lightpush endpoints are not available. Please check your configuration: --lightpushnode"

  info "REST services are installed"
  return ok()
