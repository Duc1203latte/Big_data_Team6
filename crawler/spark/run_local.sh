#!/usr/bin/env bash
# Chạy một job PySpark ngay trên máy mình (Spark local mode), không cần cụm, HDFS hay Docker.
# Dùng khi phát triển trên bản sao Silver (README mục 4.2). Lên máy chính thì dùng spark/submit.sh,
# job giữ nguyên, chỉ đổi đường dẫn.
#
# Cần (trong .venv): pip install -r requirements.txt -r requirements-gold.txt -r requirements-spark-local.txt
# và Java 17 (JAVA_HOME hoặc java trong PATH).
#
# Dùng (trong thư mục crawler/):
#   bash spark/run_local.sh jobs/price_baseline.py \
#       --silver-root ~/data/silver_20261116 --gold-root datalake_local/gold
#
# Biến môi trường tuỳ chọn: SPARK_LOCAL_CORES (mặc định 4), SPARK_DRIVER_MEMORY (mặc định 3g).

set -euo pipefail

CRAWLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CRAWLER_DIR"

if [[ $# -lt 1 ]]; then
  echo "Usage: bash spark/run_local.sh <script.py relative to crawler/> [script args...]" >&2
  exit 2
fi
SCRIPT="$1"
shift

PYTHON="${PYTHON:-python}"
SPARK_HOME="$("$PYTHON" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk@17 ]]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17      # Java 17 cài bằng Homebrew trên macOS
fi

export PYTHONPATH="$CRAWLER_DIR${PYTHONPATH:+:$PYTHONPATH}"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[${SPARK_LOCAL_CORES:-4}]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-3g}" \
  --packages io.delta:delta-spark_2.12:3.3.2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.ui.showConsoleProgress=false \
  "$SCRIPT" "$@"
