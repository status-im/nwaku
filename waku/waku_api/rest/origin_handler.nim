{.push raises: [].}

import
  std/[options, strutils, net],
  regex,
  results,
  chronicles,
  chronos,
  chronos/apps/http/httpserver

type OriginHandlerMiddlewareRef* = ref object of HttpServerMiddlewareRef
  allowedOriginMatcher: Option[Regex2]
  everyOriginAllowed: bool

proc isEveryOriginAllowed(maybeAllowedOrigin: Option[string]): bool =
  return maybeAllowedOrigin.isSome() and maybeAllowedOrigin.get() == "*"

proc compileOriginMatcher(maybeAllowedOrigin: Option[string]): Option[Regex2] =
  if maybeAllowedOrigin.isNone():
    return none(Regex2)

  let allowedOrigin = maybeAllowedOrigin.get()

  if (len(allowedOrigin) == 0):
    return none(Regex2)

  try:
    var matchOrigin: string

    if allowedOrigin == "*":
      matchOrigin = r".*"
      return some(re2(matchOrigin, {regexCaseless, regexExtended}))

    let allowedOrigins = allowedOrigin.split(",")

    var matchExpressions: seq[string] = @[]

    var prefix: string
    for allowedOrigin in allowedOrigins:
      if allowedOrigin.startsWith("http://"):
        prefix = r"http:\/\/"
        matchOrigin = allowedOrigin.substr(7)
      elif allowedOrigin.startsWith("https://"):
        prefix = r"https:\/\/"
        matchOrigin = allowedOrigin.substr(8)
      else:
        prefix = r"https?:\/\/"
        matchOrigin = allowedOrigin

      matchOrigin = matchOrigin.replace(".", r"\.")
      matchOrigin = matchOrigin.replace("*", ".*")
      matchOrigin = matchOrigin.replace("?", ".?")

      matchExpressions.add("^" & prefix & matchOrigin & "$")

    let finalExpression = matchExpressions.join("|")

    return some(re2(finalExpression, {regexCaseless, regexExtended}))
  except RegexError:
    var msg = getCurrentExceptionMsg()
    error "Failed to compile regex", source = allowedOrigin, err = msg
    return none(Regex2)

proc originsMatch(
    originHandler: OriginHandlerMiddlewareRef, requestOrigin: string
): bool =
  if originHandler.allowedOriginMatcher.isNone():
    return false

  return requestOrigin.match(originHandler.allowedOriginMatcher.get())

proc originMiddlewareProc(
    middleware: HttpServerMiddlewareRef,
    reqfence: RequestFence,
    nextHandler: HttpProcessCallback2,
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  if reqfence.isErr():
    # Ignore request errors that detected before our middleware.
    # Let final handler deal with it.
    return await nextHandler(reqfence)

  let self = OriginHandlerMiddlewareRef(middleware)
  let request = reqfence.get()
  var reqHeaders = request.headers
  var response = request.getResponse()

  if self.allowedOriginMatcher.isSome():
    let origin = reqHeaders.getList("Origin")
    try:
      if origin.len == 1:
        if self.everyOriginAllowed:
          response.addHeader("Access-Control-Allow-Origin", "*")
          response.addHeader("Access-Control-Allow-Headers", "Content-Type")
        elif self.originsMatch(origin[0]):
          # The Vary: Origin header to must be set to prevent
          # potential cache poisoning attacks:
          # https://textslashplain.com/2018/08/02/cors-and-vary/
          response.addHeader("Vary", "Origin")
          response.addHeader("Access-Control-Allow-Origin", origin[0])
          response.addHeader("Access-Control-Allow-Headers", "Content-Type")
        else:
          return await request.respond(Http403, "Origin not allowed")
      elif origin.len == 0:
        discard
      elif origin.len > 1:
        return await request.respond(
          Http400, "Only a single Origin header must be specified"
        )
    except HttpWriteError as exc:
      # We use default error handler if we unable to send response.
      return defaultResponse(exc)

  # Calling next handler.
  return await nextHandler(reqfence)

proc new*(
    t: typedesc[OriginHandlerMiddlewareRef],
    allowedOrigin: Option[string] = none(string),
): HttpServerMiddlewareRef =
  let middleware = OriginHandlerMiddlewareRef(
    allowedOriginMatcher: compileOriginMatcher(allowedOrigin),
    everyOriginAllowed: isEveryOriginAllowed(allowedOrigin),
    handler: originMiddlewareProc,
  )
  return HttpServerMiddlewareRef(middleware)
