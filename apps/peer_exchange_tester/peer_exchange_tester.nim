import random, chronos, chronicles, stew/byteutils, os
import libp2p/peerId
import
  waku/[
    waku_peer_exchange,
    node/peer_manager,
    node/peer_manager/peer_store/waku_peer_storage,
    node/peer_manager/peer_manager,
  ]
import waku/[waku_peer_exchange, node/peer_manager]
import tests/testlib/wakucore

proc connectToPeer(sw: Switch, peerAddr: string, peerId: string) {.async.} =
  let ma = MultiAddress.init(peerAddr).tryGet()
  let peerId = PeerId.init(peerId).tryGet()
  await sw.connect(peerId, @[ma])
  let conn = await sw.dial(peerId, @[ma], "/vac/waku/peer-exchange/2.0.0-alpha1")
  if conn.isNil:
    echo "--------> Connection failed: No connection returned."
  else:
    echo "--------> Connection successful: "

proc main() {.async.} =
  echo "main started"
  let addrs = "/ip4/178.128.141.171/tcp/30303/"
  let id = "16Uiu2HAkykgaECHswi3YKJ5dMLbq2kPVCo89fcyTd38UcQD6ej5W"
  let switch = newStandardSwitch()
  discard switch.start()

  await connectToPeer(switch, addrs, id)
  if len(switch.connectedPeers(Direction.Out)) > 0:
    echo "Successfully out connected with Waku test fleet"
  elif len(switch.connectedPeers(Direction.In)) > 0:
    echo "Successfully in connected with Waku test fleet"
  else:
    echo "Not connected to any peers"

  echo "\n ---------------------------------- connection completed -------------------------------- \n"

  let database = SqliteDatabase.new(":memory:").tryGet()
  let storage = WakuPeerStorage.new(database).tryGet()
  let peerManager = PeerManager.new(switch = switch, storage = storage)
  let wakuPx = WakuPeerExchange(peerManager: peerManager)

  for i in 1 .. 20:
    echo "Seq No :- " & $i & " ---> "
    echo $wakuPx.enrCache

    let res1 = await wakuPx.request(1)
    if res1.isOk:
      echo "response count :- " & $res1.get().peerInfos.len
    else:
        echo "request isn't ok"
    
    sleep(120000)

  echo "---------------------------- Done ------------------------------- "

when isMainModule:
  waitFor main()
