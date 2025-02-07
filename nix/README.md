# Usage

## Shell

A development shell can be started using:
```sh
nix develop
```

## Building

To build a Codex you can use:
```sh
nix build '.?submodules=1#default'
```
The `?submodules=1` part should eventually not be necessary.
For more details see:
https://github.com/NixOS/nix/issues/4423

It can be also done without even cloning the repo:
```sh
nix build 'git+https://github.com/waku-org/nwaku?submodules=1#'
```

## Running

```sh
nix run 'git+https://github.com/waku-org/nwaku?submodules=1#''
```

## Testing

```sh
nix flake check ".?submodules=1#"
```
