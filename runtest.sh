#!/bin/sh

set -e

# Building C programs
gcc -o db -Wall -O0 ./c/db.c

# Building Rust programs
cargo build

# Running unittest
python3.7 -m unittest
TARGET=./target/debug/db_tutorial python3.7 -m unittest
