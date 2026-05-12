#!/usr/bin/env bash
# Download TLA+ tools and run the model checker on both specs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TLA_DIR="$SCRIPT_DIR/../tla"
TOOLS_JAR="$SCRIPT_DIR/tla2tools.jar"

TLA_VERSION="1.8.0"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"

if [[ ! -f "$TOOLS_JAR" ]]; then
    echo "Downloading TLA+ tools v${TLA_VERSION}..."
    curl -fsSL -o "$TOOLS_JAR" "$TLA_URL"
fi

JVM_OPTS="-Xmx2g -XX:+UseParallelGC"

run_tlc() {
    local spec="$1"
    local cfg="$2"
    echo ""
    echo "=== Running TLC on $spec ==="
    java $JVM_OPTS -jar "$TOOLS_JAR" \
        -config "$TLA_DIR/$cfg" \
        -workers auto \
        "$TLA_DIR/$spec" 2>&1
}

run_tlc FlexiblePaxos.tla MC.cfg
run_tlc FlexiblePaxosReconfig.tla MC_Reconfig.cfg
