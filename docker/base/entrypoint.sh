#!/bin/bash

echo $@

die() {
  exit 143; # 128 + 15 -- SIGTERM
}

trap 'die' SIGINT

make "$@"
