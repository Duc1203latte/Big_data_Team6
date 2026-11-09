#!/usr/bin/env bash
# Tạo cây thư mục HDFS của giai đoạn 2 (README-2A mục 9, 10). Chạy lại bao nhiêu lần cũng được (mkdir -p).
#
#   /datalake/bronze       dữ liệu thô từ Kafka (kafka_to_bronze)
#   /datalake/silver       dữ liệu đã làm sạch, Delta (rebuild_silver)
#   /datalake/quarantine   bản ghi lỗi
#   /checkpoints           checkpoint của Spark Structured Streaming (KHÔNG BAO GIỜ XOÁ)
#
# Chạy: bash infra/hdfs/init_dirs.sh   (HDFS phải đang chạy: docker compose --profile hdfs up -d)

set -euo pipefail

CONTAINER="${NAMENODE_CONTAINER:-namenode}"
DIRS=(/datalake/bronze /datalake/silver /datalake/quarantine /checkpoints)
EXPECTED_DATANODES=2

if ! docker exec "$CONTAINER" true 2>/dev/null; then
  echo "Container '$CONTAINER' is not running. Start it first: docker compose --profile hdfs up -d" >&2
  exit 1
fi

hdfs() {
  docker exec "$CONTAINER" hdfs "$@"
}

# NameNode mới bật ở chế độ safe mode (chỉ đọc) tới khi đủ DataNode báo cáo block -> chờ thoát
hdfs dfsadmin -safemode wait

live="$(hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name: ' || true)"
if [[ "$live" -lt "$EXPECTED_DATANODES" ]]; then
  echo "WARNING: only $live live DataNode(s), expected $EXPECTED_DATANODES (replication=2 needs both)" >&2
fi

hdfs dfs -mkdir -p "${DIRS[@]}"
echo "Live DataNodes: $live"
hdfs dfs -ls / /datalake
