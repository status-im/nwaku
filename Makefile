# Copyright (c) 2022 Status Research & Development GmbH. Licensed under
# either of:
# - Apache License, version 2.0
# - MIT license
# at your option. This file may not be copied, modified, or distributed except
# according to those terms.
BUILD_SYSTEM_DIR := vendor/nimbus-build-system
EXCLUDED_NIM_PACKAGES := vendor/nim-dnsdisc/vendor
LINK_PCRE := 0

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


##########
## Main ##
##########
.PHONY: all test update clean v1 v2 test1 test2

# default target, because it's the first one that doesn't start with '.'
all: | v1 v2

test: | test1 test2
test1: | testcommon testwhisper testwaku1
test2: | testcommon testwaku2

v1: | wakunode1 example1 sim1
v2: | wakunode2 example2 wakubridge chat2 chat2bridge

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

clean: | clean-libbacktrace


##################
## Experimental ##
##################
.PHONY: librln

EXPERIMENTAL ?= false
EXPERIMENTAL_PARAMS ?= $(EMPTY)

ifeq ($(EXPERIMENTAL), true)
RLN := true
endif

### RLN

LIBRLN_BUILDDIR := $(CURDIR)/vendor/zerokit

ifeq ($(OS),Windows_NT)
LIBRLN_FILE := rln.lib
else
LIBRLN_FILE := librln.a
endif

$(LIBRLN_BUILDDIR)/$(LIBRLN_FILE):
	echo -e $(BUILD_MSG) "$@" && \
		./scripts/build_rln.sh $(LIBRLN_BUILDDIR)

ifneq ($(RLN), true)
librln: ; # noop
else
EXPERIMENTAL_PARAMS += -d:rln --passL:$(LIBRLN_FILE) --passL:-lm
librln: $(LIBRLN_BUILDDIR)/$(LIBRLN_FILE)
endif

clean-librln:
	cargo clean --manifest-path vendor/zerokit/rln/Cargo.toml

# Extend clean target
clean: | clean-librln


#################
## Waku Common ##
#################
.PHONY: testcommon

testcommon: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim testcommon $(NIM_PARAMS) waku.nims


#############
## Waku v2 ##
#############
.PHONY: testwaku2 wakunode2 testwakunode2 example2 wakubridge testbridge chat2 chat2bridge

testwaku2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim test2 $(NIM_PARAMS) $(EXPERIMENTAL_PARAMS) waku.nims

wakunode2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim wakunode2 $(NIM_PARAMS) $(EXPERIMENTAL_PARAMS) waku.nims

testwakunode2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim testwakunode2 $(NIM_PARAMS) $(EXPERIMENTAL_PARAMS) waku.nims

example2: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim example2 $(NIM_PARAMS) waku.nims

wakubridge: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim bridge $(NIM_PARAMS) waku.nims

testbridge: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim testbridge $(NIM_PARAMS) $(EXPERIMENTAL_PARAMS) waku.nims

chat2: | build deps librln
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim chat2 $(NIM_PARAMS) $(EXPERIMENTAL_PARAMS) waku.nims

chat2bridge: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim chat2bridge $(NIM_PARAMS) waku.nims


###################
## Waku v2 tools ##
###################
.PHONY: tools wakucanary networkmonitor

tools: networkmonitor wakucanary

wakucanary: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim wakucanary $(NIM_PARAMS) waku.nims

networkmonitor: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim networkmonitor $(NIM_PARAMS) waku.nims


#################
## Waku legacy ##
#################
.PHONY: testwhisper testwaku1 wakunode1 example1 sim1

testwhisper: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim testwhisper $(NIM_PARAMS) waku.nims

testwaku1: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim test1 $(NIM_PARAMS) waku.nims

wakunode1: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim wakunode1 $(NIM_PARAMS) waku.nims

example1: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim example1 $(NIM_PARAMS) waku.nims

sim1: | build deps wakunode1
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim sim1 $(NIM_PARAMS) waku.nims


###################
## Documentation ##
###################
.PHONY: docs

# TODO: Remove unused target
docs: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim doc --run --index:on --project --out:.gh-pages waku/waku.nim waku.nims


#####################
## Container image ##
#####################
# -d:insecure - Necessary to enable Prometheus HTTP endpoint for metrics
# -d:chronicles_colors:none - Necessary to disable colors in logs for Docker
DOCKER_IMAGE_NIMFLAGS ?= -d:chronicles_colors:none -d:insecure
DOCKER_IMAGE_NIMFLAGS := $(DOCKER_IMAGE_NIMFLAGS) $(HEAPTRACK_PARAMS)

# build a docker image for the fleet
docker-image: MAKE_TARGET ?= wakunode2
docker-image: DOCKER_IMAGE_TAG ?= $(MAKE_TARGET)-$(GIT_VERSION)
docker-image: DOCKER_IMAGE_NAME ?= statusteam/nim-waku:$(DOCKER_IMAGE_TAG)
docker-image:
	docker build \
		--build-arg="MAKE_TARGET=$(MAKE_TARGET)" \
		--build-arg="NIMFLAGS=$(DOCKER_IMAGE_NIMFLAGS)" \
		--build-arg="EXPERIMENTAL=$(EXPERIMENTAL)" \
		--build-arg="NIM_COMMIT=$(DOCKER_NIM_COMMIT)" \
		--label="commit=$(GIT_VERSION)" \
		--target $(TARGET) \
		--tag $(DOCKER_IMAGE_NAME) .

docker-push:
	docker push $(DOCKER_IMAGE_NAME)


################
## C Bindings ##
################
.PHONY: cbindings cwaku_example libwaku.a

libwaku.a: | build deps
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim libwaku $(NIM_PARAMS) $(EXPERIMENTAL_PARAMS) waku.nims

libwaku.so: | build deps
# TODO: pending to enhance this part. Kindly use the static approach.
	echo -e $(BUILD_MSG) "build/$@" && \
		$(ENV_SCRIPT) nim c --app:lib --opt:size --noMain --header -o:build/$@ library/cwakunode.nim

cbindings: | build libwaku.a

cwaku_example: | build cbindings
	echo -e $(BUILD_MSG) "build/$@" && \
		cp nimcache/release/libwaku/libwaku.h ./examples/cbindings/ && \
		cc -o "build/$@" \
		./examples/cbindings/waku_example.c \
		-lwaku -Lbuild/ -pthread -ldl -lm \
		-lminiupnpc -Lvendor/nim-nat-traversal/vendor/miniupnp/miniupnpc/build/ \
		-lnatpmp -Lvendor/nim-nat-traversal/vendor/libnatpmp-upstream/ \
		vendor/nim-libbacktrace/libbacktrace_wrapper.o \
		vendor/nim-libbacktrace/install/usr/lib/libbacktrace.a

endif # "variables.mk" was not included

##############################################
# Static Binaries for Multiple Architectures #
##############################################

MAKE_PID := $(shell echo $$PPID)
JOB_FLAG := $(filter -j%, $(subst --jobs ,-j,$(subst --jobs=,-j,$(subst -j ,-j,$(shell ps T | grep "^\s*$(MAKE_PID).*$(MAKE)")))))
JOBS := $(subst -j,,$(JOB_FLAG))
ifneq ($(JOBS), )
	JOBS := -j $(JOBS)
endif

ARCH :=amd64
#FIXME!
NWAKU_BUILDER_IMAGE := quay.io/vpavlin0/nwaku-builder
NWAKU_BASE_TAG := 20230512110759
NWAKU_DIST_TAG := $(GIT_VERSION)_
#nwaku-base-20230502082624_arm64

SHELL := /bin/bash

BINARIES := wakunode2 wakunode1 chat2

static-base: 
	set -x &&\
	mkdir -p $${PWD}/vendor-$(ARCH) $${PWD}/build/$(ARCH) $${PWD}/nimcache-$(ARCH) &&\
	docker run -it --rm\
	 -v $${PWD}:/home/user/nwaku:z\
	 -v $${PWD}/vendor-$(ARCH):/home/user/nwaku/vendor:z\
	 -v $${PWD}/nimcache-$(ARCH):/home/user/nwaku/nimcache:z\
	 -v $${PWD}/build/$(ARCH):/home/user/nwaku/build:z\
	 --workdir /home/user/nwaku\
	 --user $$(id -u):$$(id -g)\
	 $(NWAKU_BUILDER_IMAGE):$(NWAKU_BASE_TAG)_$(ARCH)\
	 	$(JOBS)\
	 	V=1\
	 	LOG_LEVEL="TRACE"\
	 	QUICK_AND_DIRTY_COMPILER=1\
		NIMFLAGS="$(NIMFLAGS)"\
		$(BINARIES)

static-arm64: ARCH :=arm64
static-arm64: NIMFLAGS := $(NIMFLAGS) --cpu:arm64 --os:linux --gcc.exe:aarch64-alpine-linux-musl-gcc --gcc.linkerexe:aarch64-alpine-linux-musl-gcc --passL:-static
static-arm64:
	$(MAKE) $(JOBS) static-base NIMFLAGS="$(NIMFLAGS)" ARCH=$(ARCH)



static-amd64: ARCH :=amd64
static-amd64: NIMFLAGS := $(NIMFLAGS) --cpu:amd64 --os:linux --gcc.exe:x86_64-alpine-linux-musl-gcc --gcc.linkerexe:x86_64-alpine-linux-musl-gcc --passL:-static
static-amd64:
	$(MAKE) $(JOBS) static-base NIMFLAGS="$(NIMFLAGS)" ARCH=$(ARCH)

static-clean:
	rm -rf build/arm64 build/amd64 vendor-arm64 vendor-amd64 


################################################
# Distributable Artifacts (tars and containers) #
################################################

dist-base:
	mkdir -p dist &&\
	pushd build/$(ARCH) &&\
	tar czf ../../dist/wakunode-$(GIT_VERSION)-$(ARCH).tgz $(BINARIES)

dist-amd64: static-amd64
	$(MAKE) $(JOBS) dist-base ARCH=amd64

dist-arm64: static-arm64
	$(MAKE) $(JOBS) dist-base ARCH=arm64

dist-container-base:
	echo $(ARCH) &&\
	ls &&\
	cd build/$(ARCH) &&\
	docker build -t nwaku:$(NWAKU_DIST_TAG)$(ARCH)  -f ../../docker/dist/Dockerfile.$(ARCH) .

dist-container-amd64: | dist-amd64
	$(MAKE) $(JOBS) dist-container-base  ARCH=amd64
	
dist-container-arm64: | dist-arm64
	$(MAKE) $(JOBS) dist-container-base ARCH=arm64

dist-container: 
	$(MAKE) $(JOBS) dist-container-amd64
	$(MAKE) $(JOBS) dist-container-arm64
	docker manifest create quay.io/vpavlin0/nwaku:multiarch \
		--amend nwaku:$(NWAKU_DIST_TAG)amd64 \
		--amend nwaku:$(NWAKU_DIST_TAG)arm64

###################
# Release Targets #
###################

release-notes:
	docker run \
		-it \
		--rm \
		-v $${PWD}:/opt/sv4git/repo:z \
		-u $(shell id -u) \
		quay.io/vpavlin0/sv4git:latest \
			release-notes
