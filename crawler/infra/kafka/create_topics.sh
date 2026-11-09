#!/usr/bin/env bash
# Tạo 5 topic của README-2A mục 6.1 trong container kafka. Chạy lại bao nhiêu lần cũng được:
#   - topic chưa có thì tạo,
#   - topic đã có thì đặt lại config (retention, cleanup) cho đúng bảng dưới,
#   - số partition lệch thì chỉ báo, KHÔNG tự sửa (giảm partition là không thể, tăng làm lệch thứ tự theo key).
#
# Chạy: bash infra/kafka/create_topics.sh   (Kafka phải đang chạy: docker compose --profile kafka up -d)

set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="localhost:9092"
BIN="/opt/kafka/bin"

DAY_MS=86400000

# tên topic | số partition | config (key=value, cách nhau bằng dấu phẩy)
# replication luôn = 1 vì chỉ có 1 broker
TOPICS=(
  "raw.price|6|cleanup.policy=delete,retention.ms=$((3 * DAY_MS))"
  "raw.product|3|cleanup.policy=compact"
  "raw.review|3|cleanup.policy=delete,retention.ms=$((7 * DAY_MS))"
  "dlq.invalid|1|cleanup.policy=delete,retention.ms=$((14 * DAY_MS))"
  "alerts.price|3|cleanup.policy=delete,retention.ms=$((7 * DAY_MS))"
)

kafka() {
  # Chạy một script của Kafka bên trong container
  local script="$1"
  shift
  docker exec "$CONTAINER" "$BIN/$script" --bootstrap-server "$BOOTSTRAP" "$@"
}

if ! docker exec "$CONTAINER" true 2>/dev/null; then
  echo "Container '$CONTAINER' is not running. Start it first: docker compose --profile kafka up -d" >&2
  exit 1
fi

existing="$(kafka kafka-topics.sh --list)"

for spec in "${TOPICS[@]}"; do
  IFS="|" read -r topic partitions configs <<< "$spec"

  if grep -qFx "$topic" <<< "$existing"; then
    # Đã có: đặt lại config cho đúng, kiểm tra số partition
    kafka kafka-configs.sh --alter --entity-type topics --entity-name "$topic" --add-config "$configs" > /dev/null
    actual="$(kafka kafka-topics.sh --describe --topic "$topic" | grep -c "Partition: ")"
    if [[ "$actual" != "$partitions" ]]; then
      echo "WARNING: $topic has $actual partitions, expected $partitions (not changed)" >&2
    fi
    echo "Updated  $topic ($configs)"
  else
    create_args=(--create --topic "$topic" --partitions "$partitions" --replication-factor 1)
    IFS="," read -ra pairs <<< "$configs"
    for pair in "${pairs[@]}"; do
      create_args+=(--config "$pair")
    done
    kafka kafka-topics.sh "${create_args[@]}" > /dev/null
    echo "Created  $topic ($partitions partitions, $configs)"
  fi
done

echo
kafka kafka-topics.sh --list
