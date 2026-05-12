#!/usr/bin/env bash
# Build script — detects LLVM and configures cmake automatically.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR/.."
BUILD_DIR="$ROOT/build"
BUILD_TYPE="${1:-Release}"

echo "==> Building query-compiler-llvm ($BUILD_TYPE)"

# ─── Find LLVM ────────────────────────────────────────────────────────────────
LLVM_CMAKE=""

# Try llvm-config to find cmake dir
for cmd in llvm-config-18 llvm-config-17 llvm-config-16 llvm-config; do
    if command -v "$cmd" &>/dev/null; then
        LLVM_PREFIX="$($cmd --prefix)"
        # cmake files are usually in lib/cmake/llvm or share/llvm/cmake
        for cand in \
            "$LLVM_PREFIX/lib/cmake/llvm" \
            "$LLVM_PREFIX/share/llvm/cmake" \
            "$($cmd --cmakedir 2>/dev/null || true)"; do
            if [ -f "$cand/LLVMConfig.cmake" ]; then
                LLVM_CMAKE="$cand"
                break 2
            fi
        done
        break
    fi
done

# Homebrew fallback (macOS)
if [ -z "$LLVM_CMAKE" ]; then
    for prefix in \
        "$(brew --prefix llvm@18 2>/dev/null || true)" \
        "$(brew --prefix llvm@17 2>/dev/null || true)" \
        "$(brew --prefix llvm 2>/dev/null || true)"; do
        if [ -n "$prefix" ] && [ -f "$prefix/lib/cmake/llvm/LLVMConfig.cmake" ]; then
            LLVM_CMAKE="$prefix/lib/cmake/llvm"
            export PATH="$prefix/bin:$PATH"
            break
        fi
    done
fi

# apt/debian llvm fallback
if [ -z "$LLVM_CMAKE" ]; then
    for v in 18 17 16 15; do
        cand="/usr/lib/llvm-$v/lib/cmake/llvm"
        if [ -f "$cand/LLVMConfig.cmake" ]; then
            LLVM_CMAKE="$cand"
            export PATH="/usr/lib/llvm-$v/bin:$PATH"
            break
        fi
    done
fi

if [ -z "$LLVM_CMAKE" ]; then
    echo "ERROR: Could not find LLVM cmake config. Install LLVM 16+ and try again."
    echo "  macOS:  brew install llvm"
    echo "  Debian: apt-get install llvm-17-dev"
    exit 1
fi

echo "==> Using LLVM cmake dir: $LLVM_CMAKE"

# ─── Configure ───────────────────────────────────────────────────────────────
cmake -S "$ROOT" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DLLVM_DIR="$LLVM_CMAKE" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -G Ninja 2>/dev/null || \
cmake -S "$ROOT" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DLLVM_DIR="$LLVM_CMAKE" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# ─── Build ────────────────────────────────────────────────────────────────────
cmake --build "$BUILD_DIR" --parallel "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"

echo ""
echo "==> Build complete. Binaries:"
echo "    $BUILD_DIR/qc       — interactive query runner"
echo "    $BUILD_DIR/bench    — TPC-H benchmark"
echo "    $BUILD_DIR/run_tests — unit tests"
echo ""
echo "Quick start:"
echo "    $BUILD_DIR/qc --demo --scale 50000"
echo "    $BUILD_DIR/bench --scale 100000"
echo "    $BUILD_DIR/run_tests"
