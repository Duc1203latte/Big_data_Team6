#!/usr/bin/env bash
# Chạy Airflow (scheduler + webserver, SQLite) ở foreground. Chạy nền lâu dài: dùng service systemd (README).
set -euo pipefail
AF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$AF_DIR/env.sh"
export PATH="$AF_DIR/.venv/bin:$PATH"      # standalone tự gọi lại lệnh `airflow` (scheduler, webserver) qua PATH
exec airflow standalone
