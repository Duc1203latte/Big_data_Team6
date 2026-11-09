#!/usr/bin/env bash
# Nộp một job PySpark lên cụm Spark standalone (README-2A mục 9, 13).
#
# Mỗi job chạy trong một container driver RIÊNG (cùng image với cụm), cùng mạng Docker bigdata_default:
#   - code của thư mục crawler/ mount chỉ đọc ở /app, PYTHONPATH=/app (import được common/...),
#   - có giới hạn RAM riêng (--mem), job streaming chạy 24/7 không ăn vào RAM của master,
#   - Spark UI của job ở http://localhost:<--ui-port> (mặc định 4040).
#
# Dùng (trong thư mục crawler/):
#   bash spark/submit.sh spark/smoke.py --hdfs-file /tmp/x.jsonl --topic raw.price   # xong thì container tự xoá
#   bash spark/submit.sh --name kafka-to-bronze --detach jobs/kafka_to_bronze.py
#                        # chạy nền, tự bật lại khi WSL khởi động; xem log: docker logs -f kafka-to-bronze
#   bash spark/submit.sh --ui-port 4041 --conf spark.cores.max=2 jobs/rebuild_silver.py --date 2026-11-08
#
# Tuỳ chọn (đặt TRƯỚC đường dẫn script):
#   --name NAME       tên container driver (mặc định spark-job-<giờ>)
#   --detach          chạy nền với restart unless-stopped (cho job streaming)
#   --ui-port PORT    cổng Spark UI trên localhost (mặc định 4040; 2 job cùng lúc phải khác cổng)
#   --mem SIZE        giới hạn RAM container driver (mặc định 1g)
#   --conf K=V        thêm cấu hình Spark (lặp lại được)

set -euo pipefail

CRAWLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Lấy SPARK_IMAGE từ infra/.env nếu có, để cụm và driver luôn cùng một image
if [[ -f "$CRAWLER_DIR/infra/.env" ]]; then
  SPARK_IMAGE="${SPARK_IMAGE:-$(grep -E '^SPARK_IMAGE=' "$CRAWLER_DIR/infra/.env" | cut -d= -f2- || true)}"
fi
IMAGE="${SPARK_IMAGE:-apache/spark:3.5.8-scala2.12-java17-python3-ubuntu}"
NETWORK="bigdata_default"
WORK_VOLUME="bigdata_spark-work"

NAME="spark-job-$(date +%Y%m%d-%H%M%S)"
DETACH=0
UI_PORT=4040
MEM=1g
CONFS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="$2"; shift 2 ;;
    --detach) DETACH=1; shift ;;
    --ui-port) UI_PORT="$2"; shift 2 ;;
    --mem) MEM="$2"; shift 2 ;;
    --conf) CONFS+=(--conf "$2"); shift 2 ;;
    -*) echo "Unknown option $1" >&2; exit 2 ;;
    *) break ;;
  esac
done

if [[ $# -lt 1 ]]; then
  echo "Usage: bash spark/submit.sh [options] <script.py relative to crawler/> [script args...]" >&2
  exit 2
fi
SCRIPT="$1"
shift
if [[ ! -f "$CRAWLER_DIR/$SCRIPT" ]]; then
  echo "Script not found: $CRAWLER_DIR/$SCRIPT" >&2
  exit 2
fi
if ! docker inspect -f '{{.State.Running}}' spark-master 2>/dev/null | grep -q true; then
  echo "Spark cluster is not running. Start it first: (cd infra && docker compose --profile spark up -d)" >&2
  exit 1
fi

if [[ "$DETACH" == 1 ]]; then
  RUN_MODE=(-d --restart unless-stopped)
else
  RUN_MODE=(--rm)
fi

exec docker run "${RUN_MODE[@]}" \
  --name "$NAME" --hostname "$NAME" \
  --network "$NETWORK" \
  --memory "$MEM" \
  -p "127.0.0.1:${UI_PORT}:4040" \
  -v "$CRAWLER_DIR":/app:ro \
  -v "$WORK_VOLUME":/opt/spark/work-dir \
  -w /opt/spark/work-dir \
  -e PYTHONPATH=/app \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
    --properties-file /app/spark/conf/spark-defaults.conf \
    --name "$NAME" \
    --conf "spark.driver.host=$NAME" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    "${CONFS[@]}" \
    "/app/$SCRIPT" "$@"
