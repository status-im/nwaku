import os

if defined(release):
  switch("nimcache", "nimcache/release/$projectName")
else:
  switch("nimcache", "nimcache/debug/$projectName")

if defined(windows):
  switch("passL", "rln.lib")
  switch("define", "postgres=false")
  # Exclude postgres-related files from compilation

  # Automatically add all vendor subdirectories
  for dir in walkDir("./vendor"):
    if dir.kind == pcDir:
      switch("path", dir.path)
      switch("path", dir.path / "src")

  switch("path", "./vendor/nim-nat-traversal")
  switch("path", "./vendor/nim-nat-traversal/vendor/libnatpmp-upstream")
  switch("path", "./vendor/nim-nat-traversal/vendor/miniupnp/miniupnpc")

  # disable timestamps in Windows PE headers - https://wiki.debian.org/ReproducibleBuilds/TimestampsInPEBinaries
  switch("passL", "-Wl,--no-insert-timestamp")
  # increase stack size
  switch("passL", "-Wl,--stack,8388608")
  # https://github.com/nim-lang/Nim/issues/4057
  --tlsEmulation:
    off
  if defined(i386):
    # set the IMAGE_FILE_LARGE_ADDRESS_AWARE flag so we can use PAE, if enabled, and access more than 2 GiB of RAM
    switch("passL", "-Wl,--large-address-aware")

  # The dynamic Chronicles output currently prevents us from using colors on Windows
  # because these require direct manipulations of the stdout File object.
  switch("define", "chronicles_colors=off")

# https://github.com/status-im/nimbus-eth2/blob/stable/docs/cpu_features.md#ssse3-supplemental-sse3
# suggests that SHA256 hashing with SSSE3 is 20% faster than without SSSE3, so
# given its near-ubiquity in the x86 installed base, it renders a distribution
# build more viable on an overall broader range of hardware.
#
if defined(disableMarchNative):
  if defined(i386) or defined(amd64):
    if defined(macosx):
      # macOS Catalina is EOL as of 2022-09
      # https://support.apple.com/kb/sp833
      # "macOS Big Sur - Technical Specifications" lists current oldest
      # supported models: MacBook (2015 or later), MacBook Air (2013 or later),
      # MacBook Pro (Late 2013 or later), Mac mini (2014 or later), iMac (2014
      # or later), iMac Pro (2017 or later), Mac Pro (2013 or later).
      #
      # These all have Haswell or newer CPUs.
      #
      # This ensures AVX2, AES-NI, PCLMUL, BMI1, and BMI2 instruction set support.
      switch("passC", "-march=haswell -mtune=generic")
      switch("passL", "-march=haswell -mtune=generic")
    else:
      if defined(marchOptimized):
        # https://github.com/status-im/nimbus-eth2/blob/stable/docs/cpu_features.md#bmi2--adx
        switch("passC", "-march=broadwell -mtune=generic")
        switch("passL", "-march=broadwell -mtune=generic")
      else:
        switch("passC", "-mssse3")
        switch("passL", "-mssse3")
elif defined(macosx) and defined(arm64):
  # Apple's Clang can't handle "-march=native" on M1: https://github.com/status-im/nimbus-eth2/issues/2758
  switch("passC", "-mcpu=apple-m1")
  switch("passL", "-mcpu=apple-m1")
else:
  if not defined(android):
    switch("passC", "-march=native")
    switch("passL", "-march=native")
  if defined(windows):
    # https://gcc.gnu.org/bugzilla/show_bug.cgi?id=65782
    # ("-fno-asynchronous-unwind-tables" breaks Nim's exception raising, sometimes)
    switch("passC", "-mno-avx512f")
    switch("passL", "-mno-avx512f")

--threads:
  on
--opt:
  speed
--excessiveStackTrace:
  on
# enable metric collection
--define:
  metrics
# for heap-usage-by-instance-type metrics and object base-type strings
--define:
  nimTypeNames

switch("define", "withoutPCRE")

# the default open files limit is too low on macOS (512), breaking the
# "--debugger:native" build. It can be increased with `ulimit -n 1024`.
if not defined(macosx) and not defined(android):
  # add debugging symbols and original files and line numbers
  --debugger:
    native
  if not (defined(windows) and defined(i386)) and not defined(disable_libbacktrace):
    # light-weight stack traces using libbacktrace and libunwind
    --define:
      nimStackTraceOverride
    switch("import", "libbacktrace")

--define:
  nimOldCaseObjects
  # https://github.com/status-im/nim-confutils/issues/9

# `switch("warning[CaseTransition]", "off")` fails with "Error: invalid command line option: '--warning[CaseTransition]'"
switch("warning", "CaseTransition:off")

# The compiler doth protest too much, methinks, about all these cases where it can't
# do its (N)RVO pass: https://github.com/nim-lang/RFCs/issues/230
switch("warning", "ObservableStores:off")

# Too many false positives for "Warning: method has lock level <unknown>, but another method has 0 [LockLevel]"
switch("warning", "LockLevel:off")

if defined(android):
  var clang = getEnv("ANDROID_COMPILER")
  var ndk_home = getEnv("ANDROID_TOOLCHAIN_DIR")
  var sysroot = ndk_home & "/sysroot"
  var cincludes = sysroot & "/usr/include/" & getEnv("ANDROID_ARCH")

  switch("clang.path", ndk_home & "/bin")
  switch("clang.exe", clang)
  switch("clang.linkerexe", clang)
  switch("passC", "--sysroot=" & sysRoot)
  switch("passL", "--sysroot=" & sysRoot)
  switch("cincludes", sysRoot & "/usr/include/")
