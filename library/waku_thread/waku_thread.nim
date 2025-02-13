{.pragma: exported, exportc, cdecl, raises: [].}
{.pragma: callback, cdecl, raises: [], gcsafe.}
{.passc: "-fPIC".}

import std/[options, atomics, os, net, locks]
import chronicles, chronos, chronos/threadsync, taskpools/channels_spsc_single, results
import waku/factory/waku, ./inter_thread_communication/waku_thread_request, ../ffi_types

type WakuContext* = object
  thread: Thread[(ptr WakuContext)]
  lock: Lock
  reqChannel: ChannelSPSCSingle[ptr WakuThreadRequest]
  reqSignal: ThreadSignalPtr
    # to inform The Waku Thread (a.k.a TWT) that a new request is sent
  reqReceivedSignal: ThreadSignalPtr
    # to inform the main thread that the request is rx by TWT
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
    await ctx.reqSignal.wait()

    if ctx.running.load == false:
      break

    ## Trying to get a request from the libwaku requestor thread
    var request: ptr WakuThreadRequest
    let recvOk = ctx.reqChannel.tryRecv(request)
    if not recvOk:
      error "waku thread could not receive a request"
      continue

    let fireRes = ctx.reqReceivedSignal.fireSync()
    if fireRes.isErr():
      error "could not fireSync back to requester thread", error = fireRes.error

    ## Handle the request
    asyncSpawn WakuThreadRequest.process(request, addr waku)

proc run(ctx: ptr WakuContext) {.thread.} =
  ## Launch waku worker
  waitFor runWaku(ctx)

proc createWakuThread*(): Result[ptr WakuContext, string] =
  ## This proc is called from the main thread and it creates
  ## the Waku working thread.
  var ctx = createShared(WakuContext, 1)
  ctx.reqSignal = ThreadSignalPtr.new().valueOr:
    return err("couldn't create reqSignal ThreadSignalPtr")
  ctx.reqReceivedSignal = ThreadSignalPtr.new().valueOr:
    return err("couldn't create reqReceivedSignal ThreadSignalPtr")
  ctx.lock.initLock()

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

  let signaledOnTime = ctx.reqSignal.fireSync().valueOr:
    return err("error in destroyWakuThread: " & $error)
  if not signaledOnTime:
    return err("failed to signal reqSignal on time in destroyWakuThread")

  joinThread(ctx.thread)
  ctx.lock.deinitLock()
  ?ctx.reqSignal.close()
  ?ctx.reqReceivedSignal.close()
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

  # This lock is only necessary while we use a SP Channel and while the signalling
  # between threads assumes that there aren't concurrent requests.
  # Rearchitecting the signaling + migrating to a MP Channel will allow us to receive
  # requests concurrently and spare us the need of locks
  ctx.lock.acquire()
  defer:
    ctx.lock.release()
  ## Sending the request
  let sentOk = ctx.reqChannel.trySend(req)
  if not sentOk:
    deallocShared(req)
    return err("Couldn't send a request to the waku thread: " & $req[])

  let fireSyncRes = ctx.reqSignal.fireSync()
  if fireSyncRes.isErr():
    deallocShared(req)
    return err("failed fireSync: " & $fireSyncRes.error)

  if fireSyncRes.get() == false:
    deallocShared(req)
    return err("Couldn't fireSync in time")

  ## wait until the Waku Thread properly received the request
  let res = ctx.reqReceivedSignal.waitSync()
  if res.isErr():
    deallocShared(req)
    return err("Couldn't receive reqReceivedSignal signal")

  ## Notice that in case of "ok", the deallocShared(req) is performed by the Waku Thread in the
  ## process proc.
  ok()
