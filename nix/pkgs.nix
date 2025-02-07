# This file controls the pinned version of nixpkgs we use for our Nix environment
# as well as which versions of package we use, including their overrides.
{ 
  config ? { },
  stableSystems ? [
    "x86_64-linux" "aarch64-linux"
  ],
}:

let
  # For testing local version of nixpkgs
  #nixpkgsSrc = (import <nixpkgs> { }).lib.cleanSource "/home/jakubgs/work/nixpkgs";

  # We follow release 24-05 of nixpkgs
  # https://github.com/NixOS/nixpkgs/releases/tag/24.05
  nixpkgsSrc = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/df27247e6f3e636c119e2610bf12d38b5e98cc79.tar.gz";
    sha256 = "sha256:0bbvimk7xb7akrx106mmsiwf9nzxnssisqmqffla03zz51d0kz2n";
  };

  # Status specific configuration defaults
  defaultConfig = {
    android_sdk.accept_license = true;
    allowUnfree = true;
  };

  # Override some packages and utilities
  pkgsOverlay = import ./overlay.nix;

  (import nixpkgsSrc) {
    config = defaultConfig // config;
    system = stableSystems;
    overlays = [ pkgsOverlay ];
  }
