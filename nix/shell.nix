{ 
  pkgs ? import ./pkgs.nix {},
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    git
    cmake
    openssl
    which
    rustup
    docker
    cargo
  ] ++ lib.optionals stdenv.isDarwin [ 
    libiconv
    darwin.apple_sdk.frameworks.Security
  ];

  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
    pkgs.pcre
  ];

  shellHook = ''
    export ANDROID_SDK_ROOT="${pkgs.androidPkgs.sdk}"
    export ANDROID_NDK_HOME="${pkgs.androidPkgs.ndk}"
    rustup default stable
  '';
}
