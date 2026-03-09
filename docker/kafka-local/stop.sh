#!/usr/bin/env bash
# Stop the local Kafka broker started by start.sh.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KAFKA_VERSION="${KAFKA_VERSION:-3.6.2}"
SCALA_VERSION="2.13"
KAFKA_DIR="$SCRIPT_DIR/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
PID_FILE="$SCRIPT_DIR/kafka.pid"

echo "Stopping Kafka..."

if [ -d "$KAFKA_DIR" ] && [ -x "$KAFKA_DIR/bin/kafka-server-stop.sh" ]; then
  "$KAFKA_DIR/bin/kafka-server-stop.sh" || true
fi

if [ -f "$PID_FILE" ]; then
  pid=$(cat "$PID_FILE")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 2
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
fi

echo "Kafka stopped."
