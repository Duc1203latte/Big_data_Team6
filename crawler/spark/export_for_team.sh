#!/usr/bin/env bash
# Đóng gói dữ liệu gửi nhóm qua Drive: MỘT file exports/data_YYYYMMDD.tar.gz (YYYYMMDD = ngày đóng gói, giờ VN).
#
#   data_YYYYMMDD/
#   ├── MANIFEST.txt     số dòng từng bảng / từng ngày để người nhận đối chiếu
#   ├── silver/          bản sao NGUYÊN BẢNG Silver (price/, product/, có _delta_log) -> thành viên 2 (Gold)
#   └── landing/         JSONL gốc của những ngày CHƯA gửi lần trước -> người làm speed layer (replay vào Kafka)
#
# Silver luôn gửi nguyên bảng (bảng Delta không ghép từng phần được). landing/ chỉ gửi phần mới: ngày đã gửi
# ghi ở state/export_last_landing_day.txt; chỉ gửi ngày UTC đã kết thúc (tới hôm qua).
# Giữ KEEP gói mới nhất trong exports/, gói cũ hơn tự xoá (đã lên Drive rồi).
#
# nightly_batch.sh tự gọi script này sau khi dựng Silver xong. Chạy tay (trong thư mục crawler/):
#   bash spark/export_for_team.sh

set -euo pipefail

CRAWLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRAWLER_DIR"
STATE_FILE="state/export_last_landing_day.txt"
KEEP=3

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) export_for_team: $*"; }

YESTERDAY=$(date -u -d "yesterday" +%F)
NAME="data_$(date +%Y%m%d)"
OUT="exports/$NAME.tar.gz"
WORK="exports/.work_$NAME"

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
LANDING_DIRS=()
LANDING_DAYS=()
while [[ ! "$DAY" > "$YESTERDAY" ]]; do
  for dir in landing/price/platform=*/date="$DAY" landing/product/platform=*/date="$DAY"; do
    [[ -d "$dir" ]] && LANDING_DIRS+=("$dir")
  done
  LANDING_DAYS+=("$DAY")
  DAY=$(date -u -d "$DAY + 1 day" +%F)
done

mkdir -p exports
rm -rf -- "${WORK:?}"
mkdir -p "$WORK/$NAME"

# 1. Bản sao Silver: HDFS -> container namenode -> máy
docker exec namenode rm -rf "/tmp/$NAME"
docker exec namenode mkdir "/tmp/$NAME"
docker exec namenode hdfs dfs -get /datalake/silver/price /datalake/silver/product "/tmp/$NAME/"
docker cp -q "namenode:/tmp/$NAME" "$WORK/$NAME/silver"
docker exec namenode rm -r "/tmp/$NAME"

# 2. MANIFEST: số dòng để người nhận kiểm tra đã giải nén đủ
{
  echo "Goi du lieu $NAME, tao luc $(date '+%Y-%m-%d %H:%M') gio VN"
  echo
  echo "silver/ (nguyen bang, du lieu toi het ngay UTC $SILVER_LAST):"
  .venv/bin/python - "$WORK/$NAME/silver" <<'EOF'
import sys
from deltalake import DeltaTable
for t in ("price", "product"):
    print(f"  {t}: {DeltaTable(f'{sys.argv[1]}/{t}').to_pyarrow_table().num_rows} dong")
EOF
  echo
  if [[ ${#LANDING_DAYS[@]} -eq 0 ]]; then
    echo "landing/: khong co ngay moi (da gui het toi $YESTERDAY)"
  else
    echo "landing/ (chi cac ngay moi, ngay UTC):"
    for d in "${LANDING_DAYS[@]}"; do
      n=$(find landing/price landing/product -path "*/date=$d/*" -name '*.jsonl' -exec cat {} + 2>/dev/null | wc -l)
      echo "  $d: $n dong"
    done
  fi
} > "$WORK/$NAME/MANIFEST.txt"

# 3. Nén thành một file: landing/ lấy thẳng từ crawler/ (đổi tiền tố thành data_.../landing/), còn lại từ WORK
tar czf "$OUT.part" --transform "s,^landing/,$NAME/landing/," \
  "${LANDING_DIRS[@]}" -C "$WORK" "$NAME"
mv "$OUT.part" "$OUT"
rm -rf -- "${WORK:?}"

if [[ ${#LANDING_DAYS[@]} -gt 0 ]]; then
  echo "$YESTERDAY" > "$STATE_FILE"
fi

# 4. Chỉ giữ KEEP gói mới nhất
ls -1t exports/data_*.tar.gz | tail -n +$((KEEP + 1)) | xargs -r rm --

log "done $OUT ($(du -h "$OUT" | cut -f1)), landing days: ${LANDING_DAYS[*]:-none}"
