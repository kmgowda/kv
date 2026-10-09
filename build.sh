#!/usr/bin/env bash
# Build kmgkv and run its test suite. Requires JDK 21 or newer (no other dependencies).
#   ./build.sh            # compile + run tests with 50 chaos seeds
#   ./build.sh 1000       # more chaos seeds
set -euo pipefail
cd "$(dirname "$0")"
JAVA_HOME="${JAVA_HOME:-}"
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
rm -rf out
"$JAVAC" -Xlint:all -Werror -d out $(find src -name '*.java')
"$JAVA" -cp out kmgkv.test.TestMain "${1:-50}"
