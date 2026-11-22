"""DAG nightly_batch: batch layer chạy mỗi đêm (tuần 3), thay cho cron spark/nightly_batch.sh.

    check_cluster -> compact_bronze -> rebuild_silver -> entity_resolution -> price_baseline -> daily_stats
                  -> export_for_team

Lịch 00:30 UTC (07:30 giờ VN): ngày UTC vừa hết lúc 07:00 VN, chờ 30 phút cho kafka_to_bronze ghi nốt.
Mỗi lần chạy xử lý đúng một ngày UTC = data_interval_start ({{ ds }}): lần chạy lúc 00:30 ngày 23/11 xử lý
ngày 22/11. catchup=True: cụm tắt mấy đêm thì Airflow tự chạy bù từng ngày, lần lượt (max_active_runs=1),
giống phần tự bù của nightly_batch.sh.

Mỗi task là một lệnh bash spark/submit.sh (job Spark chạy trong container driver riêng, cùng mạng Docker với
cụm), nên Airflow chỉ cần chạy trong WSL với quyền docker. Cài đặt: airflow/README.md.
Mọi job đều ghi lại trọn ngày/bảng nên Airflow thử lại (retries) hay chạy lại tay đều không sinh dữ liệu trùng.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG

try:                                            # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                             # Airflow 2
    from airflow.operators.bash import BashOperator

# crawler/airflow/dags/nightly_batch.py -> crawler/
CRAWLER_DIR = os.environ.get("BIGDATA_CRAWLER_DIR", str(Path(__file__).resolve().parents[2]))
# Ngày đầu tiên DAG tự chạy. Các ngày trước đó đã dựng bằng cron/tay; đổi nếu bật DAG muộn hơn.
START = pendulum.datetime(2026, 11, 22, tz="UTC")

SUBMIT = "bash spark/submit.sh --ui-port 4042"
DAY = "{{ ds }}"
PREV_DAY = "{{ macros.ds_add(ds, -1) }}"
RUN = "{{ ds_nodash }}"

default_args = {
    "owner": "thanh_vien_2",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "cwd": CRAWLER_DIR,
}

with DAG(
    dag_id="nightly_batch",
    description="Bronze -> Silver -> Gold -> ClickHouse batch view, mỗi đêm",
    schedule="30 0 * * *",
    start_date=START,
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    tags=["batch", "gold"],
) as dag:
    # Cụm tắt (máy chính tắt Docker để lấy RAM) thì dừng sớm, Airflow thử lại sau 10 phút rồi báo lỗi;
    # bật cụm xong chỉ cần Clear task để chạy lại.
    check_cluster = BashOperator(
        task_id="check_cluster",
        bash_command=(
            "for c in namenode datanode1 datanode2 spark-master spark-worker kafka-to-bronze qdrant clickhouse redis; do "
            "docker inspect -f '{{ '{{' }}.State.Running{{ '}}' }}' $c 2>/dev/null | grep -q true "
            "|| { echo \"container $c is not running\"; exit 1; }; done"
        ),
    )

    compact_bronze = BashOperator(
        task_id="compact_bronze",
        bash_command=f"{SUBMIT} --name nb-compact-{RUN} --mem 1536m jobs/compact_bronze.py --date {DAY}",
    )

    # Làm lại cả ngày hôm trước để nhặt event vào Bronze muộn (như nightly_batch.sh); xong ghi mốc cho
    # các công cụ cũ đọc state/nightly_batch_last_day.txt
    rebuild_silver = BashOperator(
        task_id="rebuild_silver",
        bash_command=(
            f"{SUBMIT} --name nb-silver-{RUN} --mem 1536m jobs/rebuild_silver.py "
            f"--date-from {PREV_DAY} --date-to {DAY} && echo {DAY} > state/nightly_batch_last_day.txt"
        ),
    )

    entity_resolution = BashOperator(
        task_id="entity_resolution",
        bash_command=(
            f"{SUBMIT} --name nb-er-{RUN} --mem 3g --conf spark.driver.memory=2g jobs/entity_resolution.py "
            "--qdrant-url http://qdrant:6333 --model-cache /opt/spark/work-dir/models"
        ),
        execution_timeout=timedelta(hours=1),
    )

    price_baseline = BashOperator(
        task_id="price_baseline",
        bash_command=f"{SUBMIT} --name nb-baseline-{RUN} --mem 1536m jobs/price_baseline.py --as-of {DAY}",
    )

    daily_stats = BashOperator(
        task_id="daily_stats",
        bash_command=(
            f"{SUBMIT} --name nb-stats-{RUN} --mem 1536m jobs/daily_stats.py "
            f"--date-from {PREV_DAY} --date-to {DAY} "
            "--clickhouse-url http://clickhouse:8123 --redis-url redis://redis:6379/0"
        ),
    )

    # Đóng gói Silver + landing cho nhóm (như cron cũ). Lỗi ở đây không ảnh hưởng dữ liệu đã dựng.
    export_for_team = BashOperator(
        task_id="export_for_team",
        # Dấu cách cuối là cố ý: lệnh kết thúc bằng ".sh" thì Airflow hiểu là đường dẫn file template
        bash_command="bash spark/export_for_team.sh ",
        retries=0,
    )

    check_cluster >> compact_bronze >> rebuild_silver >> entity_resolution >> price_baseline >> daily_stats
    daily_stats >> export_for_team
