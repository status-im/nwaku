{
  config ? {},
  pkgs ? import ./pkgs.nix { inherit config; },
  src ? ../.,
  targets ? ["libwaku-android-amd64"],
  verbosity ? 2,
  useSystemNim ? false,
  stableSystems ? [
    "x86_64-linux" "aarch64-linux"
  ],
  androidArch,
  zerokitPkg,
}:

assert pkgs.lib.assertMsg ((src.submodules or true) == true)
  "Unable to build without submodules. Append '?submodules=1#' to the URI.";

let
  inherit (pkgs) stdenv lib writeScriptBin callPackage;

  revision = lib.substring 0 8 (src.rev or "dirty");

in stdenv.mkDerivation rec {

  pname = "nwaku";

  version = "1.0.0-${revision}";

  inherit src;

  buildInputs = with pkgs; [
    openssl
    gmp
  ];

  # Dependencies that should only exist in the build environment.
  nativeBuildInputs = let
    # Fix for Nim compiler calling 'git rev-parse' and 'lsb_release'.
    fakeGit = writeScriptBin "git" "echo ${version}";
    # Fix for the zerokit package that is built with cargo/rustup/cross.
    fakeCargo = writeScriptBin "cargo" "echo ${version}";
    # Fix for the zerokit package that is built with cargo/rustup/cross.
    fakeRustup = writeScriptBin "rustup" "echo ${version}";
    # Fix for the zerokit package that is built with cargo/rustup/cross.
    fakeCross = writeScriptBin "cross" "echo ${version}";
  in
    with pkgs; [
      cmake
      which
      lsb-release
      zerokitPkg
      fakeGit
      fakeCargo
      fakeRustup
      fakeCross
  ];

  # Environment variables required for Android builds
  ANDROID_SDK_ROOT="${pkgs.androidPkgs.sdk}";
  ANDROID_NDK_HOME="${pkgs.androidPkgs.ndk}";
  NIMFLAGS = "-d:disableMarchNative -d:git_revision_override=${revision}";
  XDG_CACHE_HOME = "/tmp";

  makeFlags = targets ++ [
    "V=${toString verbosity}"
  ];

  configurePhase = ''
    patchShebangs . vendor/nimbus-build-system > /dev/null
    make nimbus-build-system-paths
  '';

  preBuild = ''
    pushd vendor/nimbus-build-system/vendor/Nim
    mkdir dist
    cp -r ${callPackage ./nimble.nix {}}    dist/nimble
    chmod 777 -R dist/nimble
    mkdir -p dist/nimble/dist
    cp -r ${callPackage ./checksums.nix {}} dist/checksums  # need both
    cp -r ${callPackage ./checksums.nix {}} dist/nimble/dist/checksums
    cp -r ${callPackage ./atlas.nix {}}     dist/atlas
    chmod 777 -R dist/atlas
    mkdir dist/atlas/dist
    cp -r ${callPackage ./sat.nix {}}       dist/nimble/dist/sat
    cp -r ${callPackage ./sat.nix {}}       dist/atlas/dist/sat
    cp -r ${callPackage ./csources.nix {}}  csources_v2
    chmod 777 -R dist/nimble csources_v2
    popd
    mkdir -p vendor/zerokit/target/${androidArch}/release
    cp ${zerokitPkg}/librln.so vendor/zerokit/target/${androidArch}/release/
  '';

  installPhase = ''
    mkdir -p $out/build/android
    cp -r ./build/android/* $out/build/android/
  '';

  meta = with pkgs.lib; {
    description = "NWaku derivation to build libwaku for mobile targets using Android NDK and Rust.";
    homepage = "https://github.com/status-im/nwaku";
    license = licenses.mit;
    platforms = stableSystems;
  };
}
