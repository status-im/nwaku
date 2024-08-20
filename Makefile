# Copyright (c) 2022 Status Research & Development GmbH. Licensed under
# either of:
# - Apache License, version 2.0
# - MIT license
# at your option. This file may not be copied, modified, or distributed except
# according to those terms.
BUILD_SYSTEM_DIR := vendor/nimbus-build-system
EXCLUDED_NIM_PACKAGES := vendor/nim-dnsdisc/vendor
LINK_PCRE := 0
LOG_LEVEL := TRACE

# we don't want an error here, so we can handle things later, in the ".DEFAULT" target
-include $(BUILD_SYSTEM_DIR)/makefiles/variables.mk


ifeq ($(NIM_PARAMS),)
# "variables.mk" was not included, so we update the submodules.
GIT_SUBMODULE_UPDATE := git submodule update --init --recursive
.DEFAULT:
	+@ echo -e "Git submodules not found. Running '$(GIT_SUBMODULE_UPDATE)'.\n"; \
		$(GIT_SUBMODULE_UPDATE); \
		echo
# Now that the included *.mk files appeared, and are newer than this file, Make will restart itself:
# https://www.gnu.org/software/make/manual/make.html#Remaking-Makefiles
#
# After restarting, it will execute its original goal, so we don't have to start a child Make here
# with "$(MAKE) $(MAKECMDGOALS)". Isn't hidden control flow great?

else # "variables.mk" was included. Business as usual until the end of this file.

ifeq ($(OS),Windows_NT)     # is Windows_NT on XP, 2000, 7, Vista, 10...
 detected_OS := Windows
else
 detected_OS := $(strip $(shell uname))
endif

##########
## Main ##
##########
.PHONY: all test update clean

# default target, because it's the first one that doesn't start with '.'
all: | wakunode2 example2 chat2 chat2bridge libwaku

test: | testcommon testwaku

waku.nims:
	ln -s waku.nimble $@

update: | update-common
	rm -rf waku.nims && \
		$(MAKE) waku.nims $(HANDLE_OUTPUT)

clean:
	rm -rf build

# must be included after the default target
-include $(BUILD_SYSTEM_DIR)/makefiles/targets.mk

## Possible values: prod; debug
TARGET ?= prod

## Git version
GIT_VERSION ?= $(shell git describe --abbrev=6 --always --tags)
## Compilation parameters. If defined in the CLI the assignments won't be executed
NIM_PARAMS := $(NIM_PARAMS) -d:git_version=\"$(GIT_VERSION)\"

## Heaptracker options
HEAPTRACKER ?= 0
HEAPTRACKER_INJECT ?= 0
ifeq ($(HEAPTRACKER), 1)
# Needed to make nimbus-build-system use the Nim's 'heaptrack_support' branch
DOCKER_NIM_COMMIT := NIM_COMMIT=heaptrack_support
TARGET := debug

ifeq ($(HEAPTRACKER_INJECT), 1)
# the Nim compiler will load 'libheaptrack_inject.so'
HEAPTRACK_PARAMS := -d:heaptracker -d:heaptracker_inject
else
# the Nim compiler will load 'libheaptrack_preload.so'
HEAPTRACK_PARAMS := -d:heaptracker
endif

endif
## end of Heaptracker options

##################
## Dependencies ##
##################
.PHONY: deps libbacktrace

rustup:
ifeq (, $(shell which cargo))
# Install Rustup if it's not installed
# -y: Assume "yes" for all prompts
# --default-toolchain stable: Install the stable toolchain
	curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
endif

anvil: rustup
ifeq (, $(shell which anvil))
# Install Anvil if it's not installed
	./scripts/install_anvil.sh
endif

deps: | deps-common nat-libs waku.nims


### nim-libbacktrace

# "-d:release" implies "--stacktrace:off" and it cannot be added to config.nims
ifeq ($(USE_LIBBACKTRACE), 0)
NIM_PARAMS := $(NIM_PARAMS) -d:debug -d:disable_libbacktrace
else
NIM_PARAMS := $(NIM_PARAMS) -d:release
endif

libbacktrace:
	+ $(MAKE) -C vendor/nim-libbacktrace --no-print-directory BUILD_CXX_LIB=0

clean-libbacktrace:
	+ $(MAKE) -C vendor/nim-libbacktrace clean $(HANDLE_OUTPUT)

# Extend deps and clean targets
ifneq ($(USE_LIBBACKTRACE), 0)
deps: | libbacktrace
endif

ifeq ($(POSTGRES), 1)
NIM_PARAMS := $(NIM_PARAMS) -d:postgres -d:nimDebugDlOpen
endif

ifeq ($(DEBUG_DISCV5), 1)
NIM_PARAMS := $(NIM_PARAMS) -d:debugDiscv5
endif

clean: | clean-libbacktrace


##################
##     RLN      ##
##################
.PHONY: librln

LIBRLN_BUILDDIR := $(CURDIR)/vendor/zerokit
LIBRLN_VERSION := v0.5.1

ifeq ($(OS),Windows_NT)
LIBRLN_FILE := rln.lib
else
LIBRLN_FILE := librln_$(LIBRLN_VERSION).a
endif

$(LIBRLN_FILE):
	echo -e $(BUILD_MSG) "$@" && \
		./scripts/build_rln.sh $(LIBRLN_BUILDDIR) $(LIBRLN_VERSION) $(LIBRLN_FILE)

librln: | $(LIBRLN_FILE)
	$(eval NIM_PARAMS += --passL:$(LIBRLN_FILE) --passL:-lm)

clean-librln:
	cargo clean --manifest-path vendor/zerokit/rln/Cargo.toml
	rm -f $(LIBRLN_FILE)

# Extend clean target
clean: | clean-librln


#################
## Waku Common ##
#################
.PHONY: testcommon

testcommon: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim testcommon $(NIM_PARAMS) waku.nims


##########
## Waku ##
##########
.PHONY: testwaku wakunode2 testwakunode2 example2 chat2 chat2bridge liteprotocoltester

# install anvil only for the testwaku target
testwaku: | build deps anvil librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim test -d:os=$(shell uname) $(NIM_PARAMS) waku.nims

wakunode2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim wakunode2 $(NIM_PARAMS) waku.nims

benchmarks: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim benchmarks $(NIM_PARAMS) waku.nims

testwakunode2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim testwakunode2 $(NIM_PARAMS) waku.nims

example2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim example2 $(NIM_PARAMS) waku.nims

chat2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim chat2 $(NIM_PARAMS) waku.nims

rln-db-inspector: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
	$(ENV_SCRIPT) nim rln_db_inspector $(NIM_PARAMS) waku.nims

chat2bridge: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim chat2bridge $(NIM_PARAMS) waku.nims

liteprotocoltester: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim liteprotocoltester $(NIM_PARAMS) waku.nims

build/%: | build deps librln
	echo -e $(BUILD_MSG) "build/$*" && \
		$(ENV_SCRIPT) nim buildone $(NIM_PARAMS) waku.nims $*

test/%: | build deps librln
	echo -e $(BUILD_MSG) "test/$*" && \
		$(ENV_SCRIPT) nim testone $(NIM_PARAMS) waku.nims $*

################
## Waku tools ##
################
.PHONY: tools wakucanary networkmonitor peer_exchange_tester

tools: networkmonitor wakucanary

wakucanary: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim wakucanary $(NIM_PARAMS) waku.nims

networkmonitor: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim networkmonitor $(NIM_PARAMS) waku.nims

peer_exchange_tester: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim peer_exchange_tester $(NIM_PARAMS) waku.nims

###################
## Documentation ##
###################
.PHONY: docs coverage

# TODO: Remove unused target
docs: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim doc --run --index:on --project --out:.gh-pages waku/waku.nim waku.nims

coverage:
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) ./scripts/run_cov.sh -y


#####################
## Container image ##
#####################
# -d:insecure - Necessary to enable Prometheus HTTP endpoint for metrics
# -d:chronicles_colors:none - Necessary to disable colors in logs for Docker
DOCKER_IMAGE_NIMFLAGS ?= -d:chronicles_colors:none -d:insecure -d:postgres
DOCKER_IMAGE_NIMFLAGS := $(DOCKER_IMAGE_NIMFLAGS) $(HEAPTRACK_PARAMS)

# build a docker image for the fleet
docker-image: MAKE_TARGET ?= wakunode2
docker-image: DOCKER_IMAGE_TAG ?= $(MAKE_TARGET)-$(GIT_VERSION)
docker-image: DOCKER_IMAGE_NAME ?= wakuorg/nwaku:$(DOCKER_IMAGE_TAG)
docker-image:
	docker build \
		--build-arg="MAKE_TARGET=$(MAKE_TARGET)" \
		--build-arg="NIMFLAGS=$(DOCKER_IMAGE_NIMFLAGS)" \
		--build-arg="NIM_COMMIT=$(DOCKER_NIM_COMMIT)" \
		--build-arg="LOG_LEVEL=$(LOG_LEVEL)" \
		--label="commit=$(shell git rev-parse HEAD)" \
		--label="version=$(GIT_VERSION)" \
		--target $(TARGET) \
		--tag $(DOCKER_IMAGE_NAME) .

docker-push:
	docker push $(DOCKER_IMAGE_NAME)


################
## C Bindings ##
################
.PHONY: cbindings cwaku_example libwaku

STATIC ?= false

libwaku: | build deps librln
		rm -f build/libwaku*
ifeq ($(STATIC), true)
		echo -e $(BUILD_MSG) "build/$@.a" && \
		$(ENV_SCRIPT) nim libwakuStatic $(NIM_PARAMS) waku.nims
else
		echo -e $(BUILD_MSG) "build/$@.so" && \
		$(ENV_SCRIPT) nim libwakuDynamic $(NIM_PARAMS) waku.nims
endif

#####################
## Mobile Bindings ##
#####################
.PHONY: libwaku-android \
				libwaku-android-precheck \
				libwaku-android-arm64 \
				libwaku-android-amd64 \
				libwaku-android-x86 \
				libwaku-android-arm \
				rebuild-nat-libs \
				build-libwaku-for-android-arch

ANDROID_TARGET ?= 30
ifeq ($(detected_OS),Darwin)
	ANDROID_TOOLCHAIN_DIR := $(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/darwin-x86_64
else
	ANDROID_TOOLCHAIN_DIR := $(ANDROID_NDK_HOME)/toolchains/llvm/prebuilt/linux-x86_64
endif

rebuild-nat-libs: | clean-cross nat-libs

libwaku-android-precheck:
ifndef ANDROID_NDK_HOME
		$(error ANDROID_NDK_HOME is not set)
endif

build-libwaku-for-android-arch:
	$(MAKE) rebuild-nat-libs CC=$(ANDROID_TOOLCHAIN_DIR)/bin/$(ANDROID_COMPILER) && \
	./scripts/build_rln_android.sh $(CURDIR)/build $(LIBRLN_BUILDDIR) $(LIBRLN_VERSION) $(CROSS_TARGET) $(ABIDIR) && \
	CPU=$(CPU) ABIDIR=$(ABIDIR) ANDROID_ARCH=$(ANDROID_ARCH) ANDROID_COMPILER=$(ANDROID_COMPILER) ANDROID_TOOLCHAIN_DIR=$(ANDROID_TOOLCHAIN_DIR) $(ENV_SCRIPT) nim libWakuAndroid $(NIM_PARAMS) waku.nims

libwaku-android-arm64: ANDROID_ARCH=aarch64-linux-android
libwaku-android-arm64: CPU=arm64
libwaku-android-arm64: ABIDIR=arm64-v8a
libwaku-android-arm64: | libwaku-android-precheck build deps
	$(MAKE) build-libwaku-for-android-arch ANDROID_ARCH=$(ANDROID_ARCH) CROSS_TARGET=$(ANDROID_ARCH) CPU=$(CPU) ABIDIR=$(ABIDIR) ANDROID_COMPILER=$(ANDROID_ARCH)$(ANDROID_TARGET)-clang

libwaku-android-amd64: ANDROID_ARCH=x86_64-linux-android
libwaku-android-amd64: CPU=amd64
libwaku-android-amd64: ABIDIR=x86_64
libwaku-android-amd64: | libwaku-android-precheck build deps
	$(MAKE) build-libwaku-for-android-arch ANDROID_ARCH=$(ANDROID_ARCH) CROSS_TARGET=$(ANDROID_ARCH) CPU=$(CPU) ABIDIR=$(ABIDIR) ANDROID_COMPILER=$(ANDROID_ARCH)$(ANDROID_TARGET)-clang

libwaku-android-x86: ANDROID_ARCH=i686-linux-android
libwaku-android-x86: CPU=i386
libwaku-android-x86: ABIDIR=x86
libwaku-android-x86: | libwaku-android-precheck build deps
	$(MAKE) build-libwaku-for-android-arch ANDROID_ARCH=$(ANDROID_ARCH) CROSS_TARGET=$(ANDROID_ARCH) CPU=$(CPU) ABIDIR=$(ABIDIR) ANDROID_COMPILER=$(ANDROID_ARCH)$(ANDROID_TARGET)-clang

libwaku-android-arm: ANDROID_ARCH=armv7a-linux-androideabi
libwaku-android-arm: CPU=arm
libwaku-android-arm: ABIDIR=armeabi-v7a
libwaku-android-arm: | libwaku-android-precheck build deps
# cross-rs target architecture name does not match the one used in android
	$(MAKE) build-libwaku-for-android-arch ANDROID_ARCH=$(ANDROID_ARCH) CROSS_TARGET=armv7-linux-androideabi CPU=$(CPU) ABIDIR=$(ABIDIR) ANDROID_COMPILER=$(ANDROID_ARCH)$(ANDROID_TARGET)-clang

libwaku-android:
	$(MAKE) libwaku-android-amd64
	$(MAKE) libwaku-android-arm64
	$(MAKE) libwaku-android-x86
# This target is disabled because on recent versions of cross-rs complain with the following error
# relocation R_ARM_THM_ALU_PREL_11_0 cannot be used against symbol 'stack_init_trampoline_return'; recompile with -fPIC
# It's likely this architecture is not used so we might just not support it.
#	$(MAKE) libwaku-android-arm

cwaku_example: | build libwaku
	echo -e $(BUILD_MSG) "build/$@" && \
		cc -o "build/$@" \
		./examples/cbindings/waku_example.c \
		./examples/cbindings/base64.c \
		-lwaku -Lbuild/ \
		-pthread -ldl -lm \
		-lminiupnpc -Lvendor/nim-nat-traversal/vendor/miniupnp/miniupnpc/build/ \
		-lnatpmp -Lvendor/nim-nat-traversal/vendor/libnatpmp-upstream/ \
		vendor/nim-libbacktrace/libbacktrace_wrapper.o \
		vendor/nim-libbacktrace/install/usr/lib/libbacktrace.a

cppwaku_example: | build libwaku
	echo -e $(BUILD_MSG) "build/$@" && \
		g++ -o "build/$@" \
		./examples/cpp/waku.cpp \
		./examples/cpp/base64.cpp \
		-lwaku -Lbuild/ \
		-pthread -ldl -lm \
		-lminiupnpc -Lvendor/nim-nat-traversal/vendor/miniupnp/miniupnpc/build/ \
		-lnatpmp -Lvendor/nim-nat-traversal/vendor/libnatpmp-upstream/ \
		vendor/nim-libbacktrace/libbacktrace_wrapper.o \
		vendor/nim-libbacktrace/install/usr/lib/libbacktrace.a

nodejswaku: | build deps
		echo -e $(BUILD_MSG) "build/$@" && \
		node-gyp build --directory=examples/nodejs/

endif # "variables.mk" was not included

###################
# Release Targets #
###################

release-notes:
	docker run \
		-it \
		--rm \
		-v $${PWD}:/opt/sv4git/repo:z \
		-u $(shell id -u) \
		docker.io/wakuorg/sv4git:latest \
			release-notes |\
			sed -E 's@#([0-9]+)@[#\1](https://github.com/waku-org/nwaku/issues/\1)@g'
# I could not get the tool to replace issue ids with links, so using sed for now,
# asked here: https://github.com/bvieira/sv4git/discussions/101

######################
###   NEGENTROPY   ###
######################
.PHONY: negentropy

## Pass libnegentropy to linker.
NIM_PARAMS := $(NIM_PARAMS) --passL:./libnegentropy.so

deps: | negentropy

clean: | negentropy-clean

negentropy:
	$(MAKE) -C vendor/negentropy/cpp && \
		cp vendor/negentropy/cpp/libnegentropy.so ./
negentropy-clean:
	$(MAKE) -C vendor/negentropy/cpp clean && \
		rm libnegentropy.so