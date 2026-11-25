#!/usr/bin/env bash
# Аналитика и рекомендации (Spark batch на кластере spark-master + spark-worker).
#
# Загрузка analytics-кластер -> HDFS работает постоянно в сервисе spark-ingest.
# recommendations.py сначала ждёт, пока Data Lake догонит Kafka (сравнивает офсеты),
# затем строит витрины и пишет рекомендации в топик recommendations основного кластера.
#
#   bash scripts/run-analytics.sh
source "$(dirname "$0")/common.sh"

SPARK_VERSION=3.5.6

step "Spark: аналитика и рекомендации"
dc exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=1g \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.sql.session.timeZone=UTC \
  /opt/analytics/recommendations.py

step "Data Lake в HDFS"
dc exec -T namenode hdfs dfs -ls -R /datalake 2>/dev/null | grep -v '_checkpoints\|crc$\|_SUCCESS' | grep '^d' | head -30 || true
