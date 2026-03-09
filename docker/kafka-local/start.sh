#!/usr/bin/env bash
# Start Kafka locally without Docker using KRaft mode (no Zookeeper needed).
# Downloads the Kafka binary on first run, then starts a single broker.
#
# The broker listens on ports 19092, 29092, and 39092 to match the default
# KafkaSettings in NucleoDB (so no configuration changes are needed).
#
# Usage:
#   ./start.sh           Start in foreground
#   ./start.sh -d        Start in background (detached)
#
# Prerequisites: Java 11+ must be installed.
# Environment:
#   KAFKA_VERSION   Override Kafka version (default: 3.6.2)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KAFKA_VERSION="${KAFKA_VERSION:-3.6.2}"
SCALA_VERSION="2.13"
KAFKA_DIR="$SCRIPT_DIR/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_DATA_DIR="$SCRIPT_DIR/kraft-data"
KAFKA_LOG_DIR="$SCRIPT_DIR/logs"
PID_FILE="$SCRIPT_DIR/kafka.pid"

MODE="${1:-}"

download_kafka() {
  if [ -d "$KAFKA_DIR" ]; then
    return 0
  fi
  echo "Downloading Apache Kafka ${KAFKA_VERSION}..."
  local url="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
  local archive_path="$SCRIPT_DIR/$KAFKA_ARCHIVE"

  if command -v curl &>/dev/null; then
    curl -fSL "$url" -o "$archive_path"
  elif command -v wget &>/dev/null; then
    wget -q "$url" -O "$archive_path"
  else
    echo "Error: curl or wget is required to download Kafka."
    exit 1
  fi

  echo "Extracting..."
  tar -xzf "$archive_path" -C "$SCRIPT_DIR"
  rm -f "$archive_path"
  echo "Kafka ${KAFKA_VERSION} installed at $KAFKA_DIR"
}

generate_kraft_config() {
  mkdir -p "$KAFKA_DATA_DIR" "$KAFKA_LOG_DIR"

  cat > "$SCRIPT_DIR/kraft-server.properties" <<CONF
# KRaft mode config for NucleoDB local development (auto-generated)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER

# Listeners - expose on all three ports that KafkaSettings expects
listeners=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL1://:19092,EXTERNAL2://:29092,EXTERNAL3://:39092
advertised.listeners=PLAINTEXT://localhost:9092,EXTERNAL1://127.0.0.1:19092,EXTERNAL2://127.0.0.1:29092,EXTERNAL3://127.0.0.1:39092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL1:PLAINTEXT,EXTERNAL2:PLAINTEXT,EXTERNAL3:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

# Tuning for local development
num.partitions=36
default.replication.factor=1
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
log.retention.hours=1

# Storage
log.dirs=${KAFKA_DATA_DIR}
num.io.threads=4
num.network.threads=2
CONF
}

format_storage() {
  if [ -f "$KAFKA_DATA_DIR/meta.properties" ]; then
    return 0
  fi
  echo "Formatting KRaft storage..."
  local cluster_id
  cluster_id=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_DIR/bin/kafka-storage.sh" format \
    -t "$cluster_id" \
    -c "$SCRIPT_DIR/kraft-server.properties"
  echo "Storage formatted with cluster ID: $cluster_id"
}

start_kafka() {
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Kafka is already running (PID $(cat "$PID_FILE"))"
    return 0
  fi

  if [ "$MODE" = "-d" ] || [ "$MODE" = "--detach" ]; then
    echo "Starting Kafka in background..."
    LOG_DIR="$KAFKA_LOG_DIR" \
    "$KAFKA_DIR/bin/kafka-server-start.sh" -daemon "$SCRIPT_DIR/kraft-server.properties"

    # kafka-server-start.sh -daemon backgrounds the process; find its PID
    sleep 2
    local pid
    pid=$(pgrep -f "kraft-server.properties" | head -1 || true)
    if [ -n "$pid" ]; then
      echo "$pid" > "$PID_FILE"
    fi

    echo "Waiting for Kafka to become ready..."
    for i in $(seq 1 30); do
      if "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo ""
        echo "Kafka is ready!"
        echo "  Broker:   127.0.0.1:19092 / 29092 / 39092"
        echo "  Logs:     $KAFKA_LOG_DIR/"
        [ -n "$pid" ] && echo "  PID:      $pid"
        return 0
      fi
      sleep 2
    done
    echo "Warning: Kafka did not become ready within 60s. Check $KAFKA_LOG_DIR/"
  else
    echo "Starting Kafka in foreground..."
    echo "  Broker will listen on: 127.0.0.1:19092 / 29092 / 39092"
    LOG_DIR="$KAFKA_LOG_DIR" \
    exec "$KAFKA_DIR/bin/kafka-server-start.sh" "$SCRIPT_DIR/kraft-server.properties"
  fi
}

download_kafka
generate_kraft_config
format_storage
start_kafka
