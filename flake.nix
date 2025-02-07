{
  description = "NWaku build flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    zerokit = {
      url = "github:vacp2p/zerokit?rev=4479810968b88d0ef92717524adf5edd23df1869";
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
        buildTarget = targets: pkgsFor.${system}.callPackage ./nix/default.nix {
          inherit stableSystems;
          src = self;
          zerokitPkg = zerokit.packages.x86_64-linux.default;
          androidArch = "aarch64-linux-android";
        };
      in rec {
        libwaku-android-arm64 = buildTarget "libwaku-android-arm64";
        default = libwaku-android-arm64;
      });

      devShells = forAllSystems (system: {
        default = pkgsFor.${system}.callPackage ./nix/shell.nix { };
      });
    };
}