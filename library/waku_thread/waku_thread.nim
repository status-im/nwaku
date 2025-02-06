{.pragma: exported, exportc, cdecl, raises: [].}
{.pragma: callback, cdecl, raises: [], gcsafe.}
{.passc: "-fPIC".}

import std/[options, atomics, os, net]
import chronicles, chronos, chronos/threadsync, taskpools/channels_spsc_single, results
import waku/factory/waku, ./inter_thread_communication/waku_thread_request, ../ffi_types

const MaxChannelItems = 1000

type WakuContext* = object
  thread: Thread[(ptr WakuContext)]
  reqChannel: Channel[ptr WakuThreadRequest]
  userData*: pointer
  eventCallback*: pointer
  eventUserdata*: pointer
  running: Atomic[bool] # To control when the thread is running

const git_version* {.strdefine.} = "n/a"
const versionString = "version / git commit hash: " & waku.git_version

proc runWaku(ctx: ptr WakuContext) {.async.} =
  ## This is the worker body. This runs the Waku node
  ## and attends library user requests (stop, connect_to, etc.)

  var waku: Waku

  while true:
    if ctx.running.load == false:
      break

    var request: ptr WakuThreadRequest
    try:
      echo "----------- before receive"
      request = ctx.reqChannel.recv()
      echo "----------- after receive"
    except Exception:
      error "exception trying to receive a request"

    ## Handle the request
    asyncSpawn WakuThreadRequest.process(request, addr waku)

proc run(ctx: ptr WakuContext) {.thread.} =
  ## Launch waku worker
  waitFor runWaku(ctx)

proc createWakuThread*(): Result[ptr WakuContext, string] =
  ## This proc is called from the main thread and it creates
  ## the Waku working thread.
  var ctx = createShared(WakuContext, 1)
  ctx.reqChannel.open(MaxChannelItems)

  ctx.running.store(true)

  try:
    createThread(ctx.thread, run, ctx)
  except ValueError, ResourceExhaustedError:
    # and freeShared for typed allocations!
    freeShared(ctx)

    return err("failed to create the Waku thread: " & getCurrentExceptionMsg())

  return ok(ctx)

proc destroyWakuThread*(ctx: ptr WakuContext): Result[void, string] =
  ctx.running.store(false)

  joinThread(ctx.thread)
  ctx.reqChannel.close()
  freeShared(ctx)

  return ok()

proc sendRequestToWakuThread*(
    ctx: ptr WakuContext,
    reqType: RequestType,
    reqContent: pointer,
    callback: WakuCallBack,
    userData: pointer,
): Result[void, string] =
  let req = WakuThreadRequest.createShared(reqType, reqContent, callback, userData)
  ## Sending the request, blocks if the channel is full
  echo "--------- before send reqType: ", $RequestType
  ctx.reqChannel.send(req)
  echo "--------- after reqType: ", $RequestType

  ## Notice that in case of "ok", the deallocShared(req) is performed by the Waku Thread in the
  ## process proc.
  ok()
