#!/usr/bin/env bash
# Batch đêm: dựng Silver cho những ngày UTC đã kết thúc (README-2A mục 10.3, bước 12).
# Tuần 3 thay bằng Airflow DAG nightly_batch.
#
# Cron chạy lúc 07:30 giờ VN (00:30 UTC): ngày UTC vừa hết lúc 07:00 VN, chờ 30 phút để Bronze
# ghi nốt (kafka_to_bronze ghi mỗi 5 phút). Chọn giờ này thay cho 01:00 của spec vì 01:00 VN vẫn là
# 18:00 UTC của ngày đang dở, "hôm trước" khi đó là D-1 -> Silver chậm 1 ngày rưỡi, lại trùng crawl_full.
#
#   30 7 * * * /home/Latte/projects/bigdata-crawler/crawler/spark/nightly_batch.sh >> /home/Latte/projects/bigdata-crawler/crawler/logs/nightly_batch.log 2>&1
#
# Tự bù ngày lỡ: state/nightly_batch_last_day.txt ghi ngày cuối đã dựng xong. Mỗi lần chạy xử lý từ
# ngày kế tiếp tới hôm qua (UTC), và luôn làm lại thêm 1 ngày trước đó để nhặt event vào Bronze muộn
# (rebuild_silver chạy lại cùng ngày ra y hệt nên làm lại không sao). Cụm tắt thì báo lỗi, KHÔNG ghi
# ngày đã xong, lần sau tự làm bù.
#
# Dùng (trong thư mục crawler/):
#   bash spark/nightly_batch.sh            # chạy thật
#   bash spark/nightly_batch.sh --dry-run  # chỉ in khoảng ngày sẽ chạy

set -euo pipefail

CRAWLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRAWLER_DIR"
STATE_FILE="state/nightly_batch_last_day.txt"
LOCK_FILE="state/nightly_batch.lock"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) nightly_batch: $*"; }

# Không cho 2 lần chạy chồng nhau (ví dụ chạy tay đúng lúc cron chạy)
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "another run is in progress, exiting"
  exit 1
fi

YESTERDAY=$(date -u -d "yesterday" +%F)
if [[ -f "$STATE_FILE" ]]; then
  LAST_DONE=$(tr -d '[:space:]' < "$STATE_FILE")
  NEXT=$(date -u -d "$LAST_DONE + 1 day" +%F)
else
  NEXT=$YESTERDAY
fi
LOOKBACK=$(date -u -d "$YESTERDAY - 1 day" +%F)
# Bắt đầu từ ngày sớm hơn trong hai mốc: ngày chưa làm đầu tiên, hoặc hôm kia (làm lại 1 ngày)
DATE_FROM=$([[ "$NEXT" < "$LOOKBACK" ]] && echo "$NEXT" || echo "$LOOKBACK")
DATE_TO=$YESTERDAY

log "last done=${LAST_DONE:-none}, will rebuild $DATE_FROM .. $DATE_TO"
if [[ "$DRY_RUN" == 1 ]]; then
  exit 0
fi

# Cần HDFS, Spark và kafka_to_bronze đang chạy (thiếu kafka_to_bronze thì Bronze hôm qua có thể chưa đủ)
for c in namenode datanode1 datanode2 spark-master spark-worker kafka-to-bronze; do
  if ! docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true; then
    log "ERROR: container $c is not running, skip (will catch up next run)"
    exit 1
  fi
done

START=$(date +%s)
bash spark/submit.sh --name nightly-batch --ui-port 4042 --mem 1536m \
  jobs/rebuild_silver.py --date-from "$DATE_FROM" --date-to "$DATE_TO"
echo "$DATE_TO" > "$STATE_FILE"
log "done $DATE_FROM .. $DATE_TO in $(( $(date +%s) - START ))s"
