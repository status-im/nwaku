{ pkgs ? import <nixpkgs> { } }:

let
  tools = pkgs.callPackage ./tools.nix {};
  sourceFile = ../vendor/nimbus-build-system/vendor/Nim/koch.nim;
in pkgs.fetchFromGitHub {
  owner = "nim-lang";
  repo = "atlas";
  rev = tools.findKeyValue "^ +AtlasStableCommit = \"([a-f0-9]+)\"$" sourceFile;
  # WARNING: Requires manual updates when Nim compiler version changes.
  hash = "sha256-G1TZdgbRPSgxXZ3VsBP2+XFCLHXVb3an65MuQx67o/k=";
}