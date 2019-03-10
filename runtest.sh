#!/bin/sh

set -e

gcc -o ./db -Wall -O0 ./c/db.c
python3.7 -m unittest

cargo build
RUST_BACKTRACE=1 TARGET=./target/debug/db_tutorial python3.7 -m unittest
