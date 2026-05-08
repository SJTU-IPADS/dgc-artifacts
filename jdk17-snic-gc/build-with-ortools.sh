#!/bin/bash

# Build OpenJDK with OR-Tools enabled.
#
# Override these env vars to point at your local installs:
#   ORTOOLS_PATH  path to the OR-Tools C++ distribution
#                 (default: ${HOME}/or-tools_x86_64_Ubuntu-22.04_cpp_v9.10.4067)
#   BOOT_JDK      path to a working JDK 17 used as the boot JDK
#                 (default: ${HOME}/jdk-17.0.7+7)

set -e

ORTOOLS_PATH="${ORTOOLS_PATH:-${HOME}/or-tools_x86_64_Ubuntu-22.04_cpp_v9.10.4067}"
BOOT_JDK="${BOOT_JDK:-${HOME}/jdk-17.0.7+7/}"
CONFIGURE_OPTS="--enable-warnings-as-errors=no --with-jvm-features=shenandoahgc --with-debug-level=release --with-boot-jdk=$BOOT_JDK --with-ortools=$ORTOOLS_PATH"


echo "=== Build OpenJDK with OR-Tools ==="
echo "OR-Tools path: $ORTOOLS_PATH"
echo ""

if [ ! -d "$ORTOOLS_PATH" ]; then
    echo "Error: OR-Tools path does not exist: $ORTOOLS_PATH"
    echo "Set ORTOOLS_PATH or edit this script."
    exit 1
fi

if [ ! -f "$ORTOOLS_PATH/include/ortools/sat/cp_model.h" ]; then
    echo "Error: OR-Tools header not found: $ORTOOLS_PATH/include/ortools/sat/cp_model.h"
    exit 1
fi

if [ ! -f "$ORTOOLS_PATH/lib/libortools.so" ]; then
    echo "Error: OR-Tools library not found: $ORTOOLS_PATH/lib/libortools.so"
    exit 1
fi

echo "OR-Tools check passed."

echo ""
echo "=== Configure OpenJDK ==="
echo "Running configure..."

bash ./configure $CONFIGURE_OPTS \
    --with-extra-ldflags="-Wl,-rpath,$ORTOOLS_PATH/lib" \
    2>&1 | tee configure.log

if [ $? -ne 0 ]; then
    echo "Error: configure failed"
    exit 1
fi

exit 0
