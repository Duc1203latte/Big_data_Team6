#!/usr/bin/env bash
# Cài Airflow 2.10.5 trong WSL (venv riêng, không chung với .venv của crawler) để chạy DAG nightly_batch.
# Chạy một lần, trong thư mục crawler/:   bash airflow/install.sh
# Xong thì bật:                            bash airflow/run.sh      (hoặc cài service systemd, xem README)

set -euo pipefail
CRAWLER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AF_DIR="$CRAWLER_DIR/airflow"
AIRFLOW_VERSION=2.10.5
PY_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

python3 -m venv "$AF_DIR/.venv"
# Airflow chỉ cài ổn định khi kèm file constraints đúng phiên bản Airflow và Python
"$AF_DIR/.venv/bin/pip" install --upgrade pip >/dev/null
"$AF_DIR/.venv/bin/pip" install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"

source "$AF_DIR/env.sh"
"$AF_DIR/.venv/bin/airflow" db migrate
echo "Done. Start with: bash airflow/run.sh   (UI: http://localhost:8088)"
