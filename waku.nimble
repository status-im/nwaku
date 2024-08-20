mode = ScriptMode.Verbose

### Package
version = "0.1.0"
author = "Status Research & Development GmbH"
description = "Waku, Private P2P Messaging for Resource-Restricted Devices"
license = "MIT or Apache License 2.0"
#bin           = @["build/waku"]

### Dependencies
requires "nim >= 2.0.8",
  "chronicles",
  "confutils",
  "chronos",
  "eth",
  "json_rpc",
  "libbacktrace",
  "nimcrypto",
  "stew",
  "stint",
  "metrics",
  "libp2p", # Only for Waku v2
  "web3",
  "presto",
  "regex",
  "db_connector"

### Helper functions
proc buildModule(filePath, params = "", lang = "c"): bool =
  if not dirExists "build":
    mkDir "build"
  # allow something like "nim nimbus --verbosity:0 --hints:off nimbus.nims"
  var extra_params = params
  for i in 2 ..< paramCount() - 1:
    extra_params &= " " & paramStr(i)

  if not fileExists(filePath):
    echo "File to build not found: " & filePath
    return false

  exec "nim " & lang & " --out:build/" & filepath & ".bin --mm:refc " & extra_params &
    " " & filePath

  # exec will raise exception if anything goes wrong
  return true

proc buildBinary(name: string, srcDir = "./", params = "", lang = "c") =
  if not dirExists "build":
    mkDir "build"
  # allow something like "nim nimbus --verbosity:0 --hints:off nimbus.nims"
  var extra_params = params
  for i in 2 ..< paramCount():
    extra_params &= " " & paramStr(i)
  exec "nim " & lang & " --out:build/" & name & " --mm:refc " & extra_params & " " &
    srcDir & name & ".nim"

proc buildLibrary(name: string, srcDir = "./", params = "", `type` = "static") =
  if not dirExists "build":
    mkDir "build"
  # allow something like "nim nimbus --verbosity:0 --hints:off nimbus.nims"
  var extra_params = params
  for i in 2 ..< paramCount():
    extra_params &= " " & paramStr(i)
  if `type` == "static":
    exec "nim c" & " --out:build/" & name &
      ".a --threads:on --app:staticlib --opt:size --noMain --mm:refc --header " &
      extra_params & " " & srcDir & name & ".nim"
  else:
    exec "nim c" & " --out:build/" & name &
      ".so --threads:on --app:lib --opt:size --noMain --mm:refc --header " & extra_params &
      " " & srcDir & name & ".nim"

proc buildMobileAndroid(srcDir = ".", params = "") =
  let cpu = getEnv("CPU")
  let abiDir = getEnv("ABIDIR")

  let outDir = "build/android/" & abiDir
  if not dirExists outDir:
    mkDir outDir

  var extra_params = params
  for i in 2 ..< paramCount():
    extra_params &= " " & paramStr(i)

  exec "nim c" & " --out:" & outDir &
    "/libwaku.so --threads:on --app:lib --opt:size --noMain --mm:refc -d:chronicles_sinks=textlines[dynamic] --header --passL:-L" &
    outdir & " --passL:-lrln --passL:-llog --cpu:" & cpu & " --os:android -d:androidNDK " &
    extra_params & " " & srcDir & "/libwaku.nim"

proc test(name: string, params = "-d:chronicles_log_level=DEBUG", lang = "c") =
  # XXX: When running `> NIM_PARAMS="-d:chronicles_log_level=INFO" make test2`
  # I expect compiler flag to be overridden, however it stays with whatever is
  # specified here.
  buildBinary name, "tests/", params
  exec "build/" & name

### Waku common tasks
task testcommon, "Build & run common tests":
  test "all_tests_common", "-d:chronicles_log_level=WARN -d:chronosStrictException"

### Waku tasks
task wakunode2, "Build Waku v2 cli node":
  let name = "wakunode2"
  buildBinary name, "apps/wakunode2/"

task benchmarks, "Some benchmarks":
  let name = "benchmarks"
  buildBinary name, "apps/benchmarks/"

task wakucanary, "Build waku-canary tool":
  let name = "wakucanary"
  buildBinary name, "apps/wakucanary/"

task peer_exchange_tester, "Build peer exchange tester tool":
  let name = "peer_exchange_tester"
  buildBinary name, "apps/peer_exchange_tester/"

task networkmonitor, "Build network monitor tool":
  let name = "networkmonitor"
  buildBinary name, "apps/networkmonitor/"

task rln_db_inspector, "Build the rln db inspector":
  let name = "rln_db_inspector"
  buildBinary name, "tools/rln_db_inspector/"

task test, "Build & run Waku tests":
  test "all_tests_waku"

task testwakunode2, "Build & run wakunode2 app tests":
  test "all_tests_wakunode2"

task example2, "Build Waku examples":
  buildBinary "publisher", "examples/"
  buildBinary "subscriber", "examples/"
  buildBinary "filter_subscriber", "examples/"
  buildBinary "lightpush_publisher", "examples/"

task chat2, "Build example Waku chat usage":
  # NOTE For debugging, set debug level. For chat usage we want minimal log
  # output to STDOUT. Can be fixed by redirecting logs to file (e.g.)
  #buildBinary name, "examples/", "-d:chronicles_log_level=WARN"

  let name = "chat2"
  buildBinary name, "apps/chat2/", "-d:chronicles_sinks=textlines[file] -d:ssl"

task chat2bridge, "Build chat2bridge":
  let name = "chat2bridge"
  buildBinary name, "apps/chat2bridge/"

task liteprotocoltester, "Build liteprotocoltester":
  let name = "liteprotocoltester"
  buildBinary name, "apps/liteprotocoltester/"

task buildone, "Build custom target":
  let filepath = paramStr(paramCount())
  discard buildModule filepath

task testone, "Test custom target":
  let filepath = paramStr(paramCount())
  if buildModule(filepath):
    exec "build/" & filepath & ".bin"

### C Bindings
task libwakuStatic, "Build the cbindings waku node library":
  let name = "libwaku"
  buildLibrary name,
    "library/",
    """-d:chronicles_line_numbers -d:chronicles_runtime_filtering=on -d:chronicles_sinks="textlines,json" -d:chronicles_default_output_device=Dynamic -d:chronicles_disabled_topics="eth,dnsdisc.client" """,
    "static"

task libwakuDynamic, "Build the cbindings waku node library":
  let name = "libwaku"
  buildLibrary name,
    "library/",
    """-d:chronicles_line_numbers -d:chronicles_runtime_filtering=on -d:chronicles_sinks="textlines,json" -d:chronicles_default_output_device=Dynamic -d:chronicles_disabled_topics="eth,dnsdisc.client" """,
    "dynamic"

### Mobile Android
task libWakuAndroid, "Build the mobile bindings for Android":
  let srcDir = "./library"
  let extraParams = "-d:chronicles_log_level=ERROR"
  buildMobileAndroid srcDir, extraParams
