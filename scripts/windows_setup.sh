#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

echo "Windows Setup Script"
echo "===================="

# Function to execute a command and check its status
execute_command() {
    echo "Executing: $1"
    if eval "$1"; then
        echo "✓ Command succeeded"
    else
        echo "✗ Command failed"
        exit 1
    fi
}

# Function to change directory safely
change_directory() {
    echo "Changing to directory: $1"
    if cd "$1"; then
        echo "✓ Changed directory successfully"
    else
        echo "✗ Failed to change directory"
        exit 1
    fi
}

# Function to build a component
build_component() {
    local dir="$1"
    local command="$2"
    local name="$3"

    echo "Building $name"
    if [ -d "$dir" ]; then
        change_directory "$dir"
        execute_command "$command"
        change_directory - > /dev/null
    else
        echo "✗ $name directory not found: $dir"
        exit 1
    fi
}

echo "1. Updating submodules"
execute_command "git submodule update --init --recursive"

echo "2. Creating tmp directory"
execute_command "mkdir -p tmp"

echo "3. Building Nim"
cd vendor/nimbus-build-system/vendor/Nim
./build_all.bat
cd ../../../../..

echo "4. changing branch"
cd "vendor/nim-nat-traversal/vendor/miniupnp"
git checkout little_chore_windows_support
cd ../../../..

echo "5. Building miniupnpc"
cd vendor/nim-nat-traversal/vendor/miniupnp/miniupnpc
./mingw32make.bat
cd ../../../../..

echo "6. Building libnatpmp"
cd ./vendor/nim-nat-traversal/vendor/libnatpmp-upstream
./build.bat
mv natpmp.a libnatpmp.a
cd ../../../../

echo "7. Building libunwind"
cd vendor/nim-libbacktrace
make all
make install/usr/lib/libunwind.a
cd ../../

echo "8. Building wakunode2"
make wakunode2 V=1

echo "Windows setup completed successfully!"
