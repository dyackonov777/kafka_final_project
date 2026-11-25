"""Перенос данных из аналитического кластера Kafka в Data Lake (HDFS).

Spark Structured Streaming читает топики, которые MirrorMaker 2 скопировал из основного
кластера (primary.products-filtered, primary.client-requests, primary.orders), и пишет их
в HDFS в Parquet:

    hdfs://namenode:8020/datalake/raw/topic=<topic>/ingest_date=<yyyy-mm-dd>/*.parquet

Подключение к аналитическому кластеру — mTLS, principal CN=analytics (только чтение primary.*).
Офсеты хранятся в checkpoint в HDFS: после перезапуска загрузка продолжается с места остановки.

В стенде работает постоянно как сервис spark-ingest (микробатч раз в 15 секунд):
    spark-submit ... ingest_to_hdfs.py --mode continuous
Разовая догрузка (если сервис spark-ingest остановлен; одновременно с ним не запускать —
у них общий checkpoint):
    spark-submit ... ingest_to_hdfs.py --mode once
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")
ANALYTICS_BOOTSTRAP = os.getenv("ANALYTICS_BOOTSTRAP", "kafka-b1:19092,kafka-b2:19092,kafka-b3:19092")
KEYSTORE_PASSWORD = os.environ.get("KEYSTORE_PASSWORD", "")
RAW_PATH = f"{HDFS_URL}/datalake/raw"
CHECKPOINT = f"{HDFS_URL}/datalake/_checkpoints/raw-ingest"
# Отфильтрованные товары, запросы клиентов и заказы (копия сырых товаров shop-products в аналитику не идёт)
TOPIC_PATTERN = r"primary\.(products-filtered|client-requests|orders)"
TRIGGER = os.getenv("INGEST_TRIGGER", "15 seconds")


def analytics_kafka_options() -> dict:
    """Параметры Kafka-клиента Spark для аналитического кластера (mTLS, CN=analytics)."""
    return {
        "kafka.bootstrap.servers": ANALYTICS_BOOTSTRAP,
        "kafka.security.protocol": "SSL",
        "kafka.ssl.truststore.type": "PKCS12",
        "kafka.ssl.truststore.location": "/opt/certs/client.truststore.p12",
        "kafka.ssl.truststore.password": KEYSTORE_PASSWORD,
        "kafka.ssl.keystore.type": "PKCS12",
        "kafka.ssl.keystore.location": "/opt/certs/analytics/analytics.keystore.p12",
        "kafka.ssl.keystore.password": KEYSTORE_PASSWORD,
        "kafka.ssl.key.password": KEYSTORE_PASSWORD,
        "kafka.ssl.endpoint.identification.algorithm": "https",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "continuous"], default="once")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("datalake-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    source = (
        spark.readStream.format("kafka")
        .options(**analytics_kafka_options())
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # value остаётся null для tombstone-сообщений: так аналитика узнаёт об удалённых товарах
    records = source.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.to_date("timestamp").alias("ingest_date"),
    )

    writer = (
        records.writeStream.format("parquet")
        .option("path", RAW_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("topic", "ingest_date")
        .outputMode("append")
        .queryName("datalake-ingest")
    )
    if args.mode == "once":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=TRIGGER)

    query = writer.start()
    print(f"[ingest] {ANALYTICS_BOOTSTRAP} /{TOPIC_PATTERN}/ -> {RAW_PATH} (mode={args.mode})", flush=True)
    query.awaitTermination()

    if args.mode == "once":
        try:
            stats = (spark.read.parquet(RAW_PATH).groupBy("topic")
                     .agg(F.count("*").alias("records"), F.max("timestamp").alias("last_message"))
                     .orderBy("topic"))
            print("[ingest] Содержимое Data Lake:")
            stats.show(truncate=False)
        except Exception as exc:  # noqa: BLE001 — при пустом озере данных parquet-каталога ещё нет
            print(f"[ingest] Data Lake пока пуст: {exc}")
    spark.stop()


if __name__ == "__main__":
    main()
