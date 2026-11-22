#!/usr/bin/env bash
# Đóng gói dữ liệu gửi nhóm qua Drive. Mỗi lần chạy ra 2 loại file trong exports/, cho 2 nhóm người nhận:
#
#   silver_YYYYMMDD.tar.gz        (YYYYMMDD = ngày đóng gói, giờ VN) -> thành viên 2 (Gold)
#   └── silver_YYYYMMDD/
#       ├── MANIFEST.txt          số dòng từng bảng để người nhận đối chiếu
#       ├── price/                bản sao NGUYÊN BẢNG Silver (có _delta_log)
#       └── product/
#
#   gold_YYYYMMDD.tar.gz          (Gold nguyên bảng: product_master, price_baseline, ...) -> tuần 4, 5
#   landing_YYYY-MM-DD.tar.gz     (một file cho mỗi ngày UTC chưa gửi) -> người làm speed layer
#   └── landing/
#       ├── _manifest/YYYY-MM-DD.txt   số dòng của ngày đó
#       ├── price/platform=*/date=YYYY-MM-DD/...
#       └── product/platform=*/date=YYYY-MM-DD/...
#
# Cấu trúc giống các file gửi lần đầu (silver_20261110, landing_2026-11-01..09): landing_*.tar.gz giải nén
# ngay trong thư mục crawler/ là file tự vào landing/ (replay_producer chỉ đọc */platform=*/date=*/hour=*/*.jsonl,
# nên _manifest/ không ảnh hưởng).
#
# Silver luôn gửi nguyên bảng (bảng Delta không ghép từng phần được). landing/ chỉ gửi ngày UTC đã kết thúc
# (tới hôm qua) mà chưa gửi; ngày đã gửi ghi ở state/export_last_landing_day.txt, cập nhật sau từng ngày.
# Trong exports/ chỉ giữ KEEP_SILVER gói Silver và KEEP_LANDING gói landing mới nhất, cũ hơn tự xoá (đã lên Drive).
#
# nightly_batch.sh tự gọi script này sau khi dựng Silver xong. Chạy tay (trong thư mục crawler/):
#   bash spark/export_for_team.sh

set -euo pipefail

CRAWLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRAWLER_DIR"
STATE_FILE="state/export_last_landing_day.txt"
KEEP_SILVER=3
KEEP_LANDING=7
KEEP_GOLD=3
GOLD_TABLES=(product_master price_baseline daily_price_stats daily_price_spread er_candidates)

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) export_for_team: $*"; }

YESTERDAY=$(date -u -d "yesterday" +%F)
SILVER_NAME="silver_$(date +%Y%m%d)"
GOLD_NAME="gold_$(date +%Y%m%d)"
WORK="exports/.work_$(date +%Y%m%d)"

# Silver chưa dựng tới hôm qua thì vẫn đóng gói, nhưng ghi rõ để khỏi gửi nhầm bản thiếu
SILVER_LAST=$(tr -d '[:space:]' < state/nightly_batch_last_day.txt 2>/dev/null || echo "unknown")
if [[ "$SILVER_LAST" != "$YESTERDAY" ]]; then
  log "WARNING: Silver is built up to $SILVER_LAST, not $YESTERDAY"
fi

# Những ngày landing chưa gửi: từ (ngày đã gửi + 1) tới hôm qua
if [[ -f "$STATE_FILE" ]]; then
  DAY=$(date -u -d "$(tr -d '[:space:]' < "$STATE_FILE") + 1 day" +%F)
else
  DAY=$YESTERDAY
fi
LANDING_DAYS=()
while [[ ! "$DAY" > "$YESTERDAY" ]]; do
  LANDING_DAYS+=("$DAY")
  DAY=$(date -u -d "$DAY + 1 day" +%F)
done

mkdir -p exports
rm -rf -- "${WORK:?}"
mkdir -p "$WORK"

# 1. Silver: HDFS -> container namenode -> máy, kèm MANIFEST, nén thành một file
docker exec namenode rm -rf "/tmp/$SILVER_NAME"
docker exec namenode mkdir "/tmp/$SILVER_NAME"
docker exec namenode hdfs dfs -get /datalake/silver/price /datalake/silver/product "/tmp/$SILVER_NAME/"
docker cp -q "namenode:/tmp/$SILVER_NAME" "$WORK/$SILVER_NAME"
docker exec namenode rm -r "/tmp/$SILVER_NAME"
{
  echo "Silver nguyen bang $SILVER_NAME, tao luc $(date '+%Y-%m-%d %H:%M') gio VN"
  echo "Du lieu tu 2026-11-01 toi het ngay UTC $SILVER_LAST (07:00 sang ngay hom sau, gio VN)"
  echo
  .venv/bin/python - "$WORK/$SILVER_NAME" <<'EOF'
import sys
from deltalake import DeltaTable
for t in ("price", "product"):
    print(f"  {t}: {DeltaTable(f'{sys.argv[1]}/{t}').to_pyarrow_table().num_rows} dong")
EOF
} > "$WORK/$SILVER_NAME/MANIFEST.txt"
tar czf "exports/$SILVER_NAME.tar.gz.part" -C "$WORK" "$SILVER_NAME"
mv "exports/$SILVER_NAME.tar.gz.part" "exports/$SILVER_NAME.tar.gz"
log "done exports/$SILVER_NAME.tar.gz ($(du -h "exports/$SILVER_NAME.tar.gz" | cut -f1)), Silver up to $SILVER_LAST"

# 1b. Gold (tuần 3): cùng cách với Silver, cho người làm speed layer (tuần 4) và serving (tuần 5).
#     Bảng nào chưa có trên HDFS thì bỏ qua; chưa có bảng nào thì không tạo gói.
gold_paths=()
for t in "${GOLD_TABLES[@]}"; do
  docker exec namenode hdfs dfs -test -d "/datalake/gold/$t" 2>/dev/null && gold_paths+=("/datalake/gold/$t")
done
if [[ ${#gold_paths[@]} -gt 0 ]]; then
  docker exec namenode rm -rf "/tmp/$GOLD_NAME"
  docker exec namenode mkdir "/tmp/$GOLD_NAME"
  docker exec namenode hdfs dfs -get "${gold_paths[@]}" "/tmp/$GOLD_NAME/"
  docker cp -q "namenode:/tmp/$GOLD_NAME" "$WORK/$GOLD_NAME"
  docker exec namenode rm -r "/tmp/$GOLD_NAME"
  {
    echo "Gold nguyen bang $GOLD_NAME, tao luc $(date '+%Y-%m-%d %H:%M') gio VN, tu Silver toi het ngay UTC $SILVER_LAST"
    echo "Schema: crawler/docs/gold_schema.md"
    echo
    .venv/bin/python - "$WORK/$GOLD_NAME" <<'EOF'
import os, sys
from deltalake import DeltaTable
for t in sorted(os.listdir(sys.argv[1])):
    if os.path.isdir(os.path.join(sys.argv[1], t, "_delta_log")):
        print(f"  {t}: {DeltaTable(os.path.join(sys.argv[1], t)).to_pyarrow_table().num_rows} dong")
EOF
  } > "$WORK/$GOLD_NAME/MANIFEST.txt"
  tar czf "exports/$GOLD_NAME.tar.gz.part" -C "$WORK" "$GOLD_NAME"
  mv "exports/$GOLD_NAME.tar.gz.part" "exports/$GOLD_NAME.tar.gz"
  log "done exports/$GOLD_NAME.tar.gz ($(du -h "exports/$GOLD_NAME.tar.gz" | cut -f1))"
else
  log "no Gold table on HDFS yet, skip gold package"
fi

# 2. landing: mỗi ngày một file, lấy thẳng từ landing/ của crawler, kèm _manifest/<ngày>.txt
for d in "${LANDING_DAYS[@]}"; do
  dirs=()
  for dir in landing/price/platform=*/date="$d" landing/product/platform=*/date="$d"; do
    [[ -d "$dir" ]] && dirs+=("$dir")
  done
  mkdir -p "$WORK/landing/_manifest"
  {
    echo "landing ngay UTC $d (07:00 ngay $d toi 07:00 ngay hom sau, gio VN), tao luc $(date '+%Y-%m-%d %H:%M') gio VN"
    for t in price product; do
      n=$(find "landing/$t" -path "*/date=$d/*" -name '*.jsonl' -exec cat {} + | wc -l)
      echo "  $t: $n dong"
    done
  } > "$WORK/landing/_manifest/$d.txt"
  out="exports/landing_$d.tar.gz"
  tar czf "$out.part" "${dirs[@]}" -C "$WORK" "landing/_manifest/$d.txt"
  mv "$out.part" "$out"
  echo "$d" > "$STATE_FILE"
  log "done $out ($(du -h "$out" | cut -f1))"
done
if [[ ${#LANDING_DAYS[@]} -eq 0 ]]; then
  log "no new landing day (already sent up to $YESTERDAY)"
fi
rm -rf -- "${WORK:?}"

# 3. Chỉ giữ các gói mới nhất (tên chứa ngày nên sắp theo tên là theo thời gian)
find exports -maxdepth 1 -name 'silver_*.tar.gz' | sort -r | tail -n +$((KEEP_SILVER + 1)) | xargs -r rm --
find exports -maxdepth 1 -name 'landing_*.tar.gz' | sort -r | tail -n +$((KEEP_LANDING + 1)) | xargs -r rm --
find exports -maxdepth 1 -name 'gold_*.tar.gz' | sort -r | tail -n +$((KEEP_GOLD + 1)) | xargs -r rm --
