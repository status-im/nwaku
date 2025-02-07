{
  description = "NWaku build flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    zerokit = {
      url = "github:vacp2p/zerokit?ref=add-nix-flake-and-derivation";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, zerokit }:
    let
      stableSystems = [
        "x86_64-linux" "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs stableSystems (system: f system);
      pkgsFor = forAllSystems (
        system: import nixpkgs {
          inherit system;
          config = {
            android_sdk.accept_license = true;
            allowUnfree = true;
          };
          overlays =  [
            (final: prev: {
              androidEnvCustom = prev.callPackage ./nix/pkgs/android-sdk { };
              androidPkgs = final.androidEnvCustom.pkgs;
              androidShell = final.androidEnvCustom.shell;
            })
          ];
        }
      );
    in rec {
      packages = forAllSystems (system: let
        zerokitPkg = zerokit.packages.${system}.default;
        buildTarget = pkgsFor.${system}.callPackage ./nix/default.nix rec {
          inherit stableSystems zerokitPkg;
          src = self;
        };
        build = targets: buildTarget.override { inherit targets; };
      in rec {
        libwaku-android-amd64 = build ["libwaku-android-amd64"];
        libwaku-android-arm64 = build ["libwaku-android-arm64"];
        libwaku-android-arm   = build ["libwaku-android-arm"];
        libwaku-android-x86   = build ["libwaku-android-x86"];
        default = libwaku-android-amd64;
      });

      devShells = forAllSystems (system: {
        default = pkgsFor.${system}.callPackage ./nix/shell.nix { };
      });
    };
}
