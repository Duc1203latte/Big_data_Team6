# Image Spark của cụm + thư viện Python cho các job Gold (tuần 3).
# Cùng base với cụm hiện tại, chỉ thêm gói Python, nên kafka_to_bronze, rebuild_silver chạy y như cũ.
#
# Build (trên máy chính, trong thư mục crawler/):
#   docker build -t bigdata-spark:3.5.8-gold infra/spark
# rồi đặt SPARK_IMAGE=bigdata-spark:3.5.8-gold trong infra/.env và tạo lại cụm:
#   cd infra && docker compose --profile spark up -d --force-recreate
# (spark/submit.sh đọc SPARK_IMAGE từ infra/.env, nên driver và cụm luôn cùng image.)

ARG SPARK_BASE=apache/spark:3.5.8-scala2.12-java17-python3-ubuntu
FROM ${SPARK_BASE}

USER root
COPY requirements.txt /tmp/requirements-gold.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-gold.txt && rm /tmp/requirements-gold.txt
USER spark
