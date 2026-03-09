#!/usr/bin/env bash
# Remove downloaded Kafka binary and all data (full reset).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Stop first if running
bash "$SCRIPT_DIR/stop.sh" 2>/dev/null || true

echo "Cleaning up Kafka installation and data..."
rm -rf "$SCRIPT_DIR"/kafka_* \
       "$SCRIPT_DIR/kraft-data" \
       "$SCRIPT_DIR/kraft-server.properties" \
       "$SCRIPT_DIR/logs" \
       "$SCRIPT_DIR/kafka.pid"
echo "Cleanup complete."
