# Nwaku

## Introduction

The nwaku repository implements Waku, and provides tools related to it.

- A Nim implementation of the [Waku (v2) protocol](https://specs.vac.dev/specs/waku/v2/waku-v2.html).
- CLI application `wakunode2` that allows you to run a Waku node.
- Examples of Waku usage.
- Various tests of above.

For more details see the [source code](waku/README.md)

## How to Build & Run ( Linux, MacOS & WSL )

These instructions are generic. For more detailed instructions, see the Waku source code above.

### Prerequisites

The standard developer tools, including a C compiler, GNU Make, Bash, and Git. More information on these installations can be found [here](https://docs.waku.org/guides/nwaku/build-source#install-dependencies).

> In some distributions (Fedora linux for example), you may need to install `which` utility separately. Nimbus build system is relying on it.

### Wakunode

```bash
# The first `make` invocation will update all Git submodules.
# You'll run `make update` after each `git pull` in the future to keep those submodules updated.
make wakunode2

# Build with custom compilation flags. Do not use NIM_PARAMS unless you know what you are doing.
# Replace with your own flags
make wakunode2 NIMFLAGS="-d:chronicles_colors:none -d:disableMarchNative"

# Run with DNS bootstrapping
./build/wakunode2 --dns-discovery --dns-discovery-url=DNS_BOOTSTRAP_NODE_URL

# See available command line options
./build/wakunode2 --help
```
To join the network, you need to know the address of at least one bootstrap node.
Please refer to the [Waku README](https://github.com/waku-org/nwaku/blob/master/waku/README.md) for more information.

For more on how to run `wakunode2`, refer to:
- [Run using binaries](https://docs.waku.org/guides/nwaku/build-source)
- [Run using docker](https://docs.waku.org/guides/nwaku/run-docker)
- [Run using docker-compose](https://docs.waku.org/guides/nwaku/run-docker-compose)

#### Issues
##### WSL
If you encounter difficulties building the project on WSL, consider placing the project within WSL's filesystem, avoiding the `/mnt/` directory.

### How to Build & Run ( Windows )

Note: This is a work in progress. The current setup procedure is as follows:
Goal: Get rid of windows specific procedures and make the build process the same as linux/macos.

The current setup procedure is as follows:

1. Clone the repository and checkout master branch
2. Ensure prerequisites are installed (Make, GCC, MSYS2/MinGW)
3. Run scripts/windows_setup.sh

### Developing

#### Nim Runtime
This repository is bundled with a Nim runtime that includes the necessary dependencies for the project.

Before you can utilize the runtime you'll need to build the project, as detailed in a previous section.
This will generate a `vendor` directory containing various dependencies, including the `nimbus-build-system` which has the bundled nim runtime.

After successfully building the project, you may bring the bundled runtime into scope by running:
```bash
source env.sh
```
If everything went well, you should see your prompt suffixed with `[Nimbus env]$`. Now you can run `nim` commands as usual.

### Waku Protocol Test Suite

```bash
# Run all the Waku tests
make test
```

### Building single test files

During development it is helpful to build and run a single test file.
To support this make has a specific target:

targets:
- `build/<relative path to your test file.nim>`
- `test/<relative path to your test file.nim>`

Binary will be created as `<path to your test file.nim>.bin` under the `build` directory .

```bash
# Build and run your test file separately
make test/tests/common/test_enr_builder.nim
```

## Formatting

Nim files are expected to be formatted using the [`nph`](https://github.com/arnetheduck/nph) version present in `vendor/nph`.

You can easily format file with the `make nph/<relative path to nim> file` command.
For example:

```
make nph/waku/waku_core.nim
```

A convenient git hook is provided to automatically format file at commit time.
Run the following command to install it:

```shell
make install-nph
```

### Examples

Examples can be found in the examples folder.
This includes a fully featured chat example.

### Tools

Different tools and their corresponding how-to guides can be found in the `tools` folder.

### Bugs, Questions & Features

For an inquiry, or if you would like to propose new features, feel free to [open a general issue](https://github.com/waku-org/nwaku/issues/new).

For bug reports, please [tag your issue with the `bug` label](https://github.com/waku-org/nwaku/issues/new).

If you believe the reported issue requires critical attention, please [use the `critical` label](https://github.com/waku-org/nwaku/issues/new?labels=critical,bug) to assist with triaging.

To get help, or participate in the conversation, join the [Waku Discord](https://discord.waku.org/) server.

### Docs

* [REST API Documentation](https://waku-org.github.io/waku-rest-api/)
