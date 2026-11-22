# Biến môi trường chung cho Airflow của dự án (install.sh, run.sh, service systemd đều source file này).
_AF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AIRFLOW_HOME="$_AF_DIR/home"                       # SQLite, log, mật khẩu admin (không commit)
export AIRFLOW__CORE__DAGS_FOLDER="$_AF_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc                # mọi mốc thời gian trong dự án là UTC
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8088           # 8080, 8081 đã dành cho Spark UI
export AIRFLOW__WEBSERVER__WEB_SERVER_HOST=127.0.0.1
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=60
export BIGDATA_CRAWLER_DIR="$(cd "$_AF_DIR/.." && pwd)"
