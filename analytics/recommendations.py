"""Аналитика и персональные рекомендации по данным Data Lake (Spark, batch).

Вход (HDFS, Parquet, наполняет ingest_to_hdfs.py):
  * primary.products-filtered — каталог разрешённых товаров;
  * primary.client-requests   — запросы клиентов (поиск, запрос рекомендаций);
  * primary.orders            — заказы клиентов.

Аналитика (сохраняется в HDFS /datalake/marts/* и печатается в консоль):
  * catalog_by_category — товары, средняя цена и остатки по категориям;
  * category_demand     — интерес клиентов к категориям (по результатам поиска);
  * top_queries         — популярные поисковые запросы;
  * sales_by_category   — заказы, проданные единицы и выручка по категориям;
  * top_products_sales  — самые продаваемые товары;
  * recommendations     — рекомендации по пользователям.

Рекомендации (простая эвристика, построение ML-модели вне рамок курса):
  * взаимодействия: товар в поисковой выдаче = вес 1, покупка товара = вес 5;
  * интерес пользователя к категории = сумма весов его взаимодействий с товарами категории;
  * популярность товара = сумма весов взаимодействий всех пользователей с товаром;
  * score = 10 * интерес_к_категории + популярность; берём TOP_N товаров в наличии,
    исключая уже купленные пользователем;
  * пользователи без поисков и покупок получают самые популярные товары.

Выход: топик recommendations основного кластера (mTLS, principal CN=analytics, ключ = user_id).
Kafka Connect переносит его в Elasticsearch, откуда рекомендации выдаёт CLIENT API.
"""
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")
RAW_PATH = f"{HDFS_URL}/datalake/raw"
MARTS_PATH = f"{HDFS_URL}/datalake/marts"

PRIMARY_BOOTSTRAP = os.getenv("PRIMARY_BOOTSTRAP", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
RECOMMENDATIONS_TOPIC = "recommendations"
KEYSTORE_PASSWORD = os.environ.get("KEYSTORE_PASSWORD", "")
TOP_N = int(os.getenv("TOP_N", "5"))
INGEST_WAIT_SECONDS = int(os.getenv("INGEST_WAIT_SECONDS", "180"))

# ingest_to_hdfs.py лежит рядом: берём из него параметры подключения к аналитическому кластеру
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PRODUCTS_TOPIC = "primary.products-filtered"
EVENTS_TOPIC = "primary.client-requests"
ORDERS_TOPIC = "primary.orders"

WEIGHT_IMPRESSION = 1   # товар попал в поисковую выдачу
WEIGHT_PURCHASE = 5     # товар купили

PRODUCT_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("price", T.StructType([
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
    ])),
    T.StructField("category", T.StringType()),
    T.StructField("brand", T.StringType()),
    T.StructField("stock", T.StructType([
        T.StructField("available", T.IntegerType()),
        T.StructField("reserved", T.IntegerType()),
    ])),
    T.StructField("sku", T.StringType()),
    T.StructField("tags", T.ArrayType(T.StringType())),
    T.StructField("store_id", T.StringType()),
    T.StructField("updated_at", T.StringType()),
])

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("query", T.StringType()),
    T.StructField("result_product_ids", T.ArrayType(T.StringType())),
    T.StructField("result_count", T.IntegerType()),
    T.StructField("created_at", T.StringType()),
])


ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("price", T.DoubleType()),
    ]))),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("created_at", T.StringType()),
])


def kafka_ssl_options() -> dict:
    """Параметры Kafka-продюсера Spark для основного кластера (mTLS, CN=analytics)."""
    return {
        "kafka.bootstrap.servers": PRIMARY_BOOTSTRAP,
        "kafka.security.protocol": "SSL",
        "kafka.ssl.truststore.type": "PKCS12",
        "kafka.ssl.truststore.location": "/opt/certs/client.truststore.p12",
        "kafka.ssl.truststore.password": KEYSTORE_PASSWORD,
        "kafka.ssl.keystore.type": "PKCS12",
        "kafka.ssl.keystore.location": "/opt/certs/analytics/analytics.keystore.p12",
        "kafka.ssl.keystore.password": KEYSTORE_PASSWORD,
        "kafka.ssl.key.password": KEYSTORE_PASSWORD,
        "kafka.ssl.endpoint.identification.algorithm": "https",
        "kafka.acks": "all",
    }


def load_products(raw: DataFrame) -> DataFrame:
    """Актуальный каталог: последняя версия каждого товара (ключ сообщения = product_id).

    Магазин может прислать товар повторно (новая цена, остатки) — берём последнее сообщение.
    Если последнее сообщение — tombstone (value = null), товар запрещён и удалён из каталога.
    """
    latest = Window.partitionBy("key").orderBy(F.col("timestamp").desc(), F.col("offset").desc())
    return (raw.filter((F.col("topic") == PRODUCTS_TOPIC) & F.col("key").isNotNull())
            .withColumn("rn", F.row_number().over(latest))
            .filter((F.col("rn") == 1) & F.col("value").isNotNull())
            .select(F.from_json("value", PRODUCT_SCHEMA).alias("p"))
            .select("p.*")
            .filter(F.col("product_id").isNotNull()))


def load_events(raw: DataFrame) -> DataFrame:
    return (raw.filter(F.col("topic") == EVENTS_TOPIC)
            .select(F.from_json("value", EVENT_SCHEMA).alias("e"))
            .select("e.*")
            .filter(F.col("user_id").isNotNull()))


def load_order_lines(raw: DataFrame) -> DataFrame:
    """Строки заказов: один товар заказа = одна строка."""
    orders = (raw.filter(F.col("topic") == ORDERS_TOPIC)
              .select(F.from_json("value", ORDER_SCHEMA).alias("o"))
              .select("o.*")
              .filter(F.col("user_id").isNotNull() & F.col("order_id").isNotNull())
              .dropDuplicates(["order_id"]))
    return (orders.select("order_id", "user_id", F.explode("items").alias("i"))
            .select("order_id", "user_id", F.col("i.product_id").alias("product_id"),
                    F.col("i.name").alias("name"), F.col("i.category").alias("category"),
                    F.coalesce(F.col("i.quantity"), F.lit(1)).alias("quantity"),
                    F.coalesce(F.col("i.price"), F.lit(0.0)).alias("price"))
            .withColumn("amount", F.col("quantity") * F.col("price")))


def save_mart(df: DataFrame, name: str, title: str) -> None:
    df.write.mode("overwrite").parquet(f"{MARTS_PATH}/{name}")
    print(f"\n=== {title}  ->  {MARTS_PATH}/{name}")
    df.show(20, truncate=False)


def wait_for_ingest(spark: SparkSession, timeout: int = INGEST_WAIT_SECONDS) -> None:
    """Ждёт, пока Data Lake догонит аналитический кластер Kafka (вместо фиксированной паузы).

    Сравнивает последний офсет каждой партиции в Kafka с последним офсетом, записанным в HDFS
    сервисом spark-ingest. Если за timeout не догнали — считаем на том, что есть, с предупреждением.
    """
    from ingest_to_hdfs import TOPIC_PATTERN, analytics_kafka_options

    kafka_end = (spark.read.format("kafka").options(**analytics_kafka_options())
                 .option("subscribePattern", TOPIC_PATTERN)
                 .option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
                 .groupBy("topic", "partition").agg(F.max("offset").alias("kafka_offset"))
                 .collect())
    target = {(r["topic"], r["partition"]): r["kafka_offset"] for r in kafka_end}
    if not target:
        print("[analytics] В аналитическом кластере пока нет данных")
        return

    deadline = time.time() + timeout
    while True:
        try:
            lake = (spark.read.parquet(RAW_PATH).groupBy("topic", "partition")
                    .agg(F.max("offset").alias("lake_offset")).collect())
            done = {(r["topic"], r["partition"]): r["lake_offset"] for r in lake}
        except Exception:  # noqa: BLE001 — каталога ещё нет
            done = {}
        behind = {tp: off - done.get(tp, -1) for tp, off in target.items() if done.get(tp, -1) < off}
        if not behind:
            print(f"[analytics] Data Lake актуален: {len(target)} партиций догнали Kafka")
            return
        if time.time() >= deadline:
            print(f"[analytics] ВНИМАНИЕ: за {timeout} с Data Lake не догнал Kafka, отставание: {behind}")
            return
        print(f"[analytics] ждём загрузку в HDFS, отстают партиции: {len(behind)}")
        time.sleep(5)


def main() -> None:
    spark = SparkSession.builder.appName("marketplace-recommendations").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    wait_for_ingest(spark)

    try:
        raw = spark.read.parquet(RAW_PATH)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"[analytics] В Data Lake нет данных ({RAW_PATH}). "
                         f"Проверьте сервис spark-ingest (docker compose logs spark-ingest). Причина: {exc}")

    products = load_products(raw).cache()
    events = load_events(raw).cache()
    order_lines = load_order_lines(raw).cache()
    print(f"[analytics] товаров в каталоге: {products.count()}, событий клиентов: {events.count()}, "
          f"строк заказов: {order_lines.count()}")

    # --- Аналитические витрины ---
    catalog_by_category = (products.groupBy("category")
                           .agg(F.count("*").alias("products"),
                                F.round(F.avg("price.amount"), 2).alias("avg_price"),
                                F.sum("stock.available").alias("stock_available"))
                           .orderBy(F.desc("products")))
    save_mart(catalog_by_category, "catalog_by_category", "Каталог по категориям")

    searches = events.filter(F.col("event_type") == "search")
    impressions = searches.select("user_id", F.explode("result_product_ids").alias("product_id"),
                                  F.lit(WEIGHT_IMPRESSION).alias("weight"))
    purchases = order_lines.select("user_id", "product_id", F.lit(WEIGHT_PURCHASE).alias("weight"))
    interactions = impressions.unionByName(purchases)

    top_queries = (searches.filter(F.col("query").isNotNull())
                   .groupBy(F.lower(F.trim("query")).alias("query"))
                   .agg(F.count("*").alias("searches"),
                        F.countDistinct("user_id").alias("users"),
                        F.round(F.avg("result_count"), 1).alias("avg_results"))
                   .orderBy(F.desc("searches"), "query"))
    save_mart(top_queries, "top_queries", "Популярные поисковые запросы")

    product_categories = products.select("product_id", "category")
    category_demand = (impressions.join(product_categories, "product_id")
                       .groupBy("category")
                       .agg(F.count("*").alias("impressions"),
                            F.countDistinct("user_id").alias("users"))
                       .orderBy(F.desc("impressions")))
    save_mart(category_demand, "category_demand", "Интерес клиентов к категориям (поиск)")

    sales_by_category = (order_lines.groupBy("category")
                         .agg(F.countDistinct("order_id").alias("orders"),
                              F.sum("quantity").alias("units"),
                              F.round(F.sum("amount"), 2).alias("revenue"),
                              F.countDistinct("user_id").alias("buyers"))
                         .orderBy(F.desc("revenue")))
    save_mart(sales_by_category, "sales_by_category", "Продажи по категориям")

    top_products_sales = (order_lines.groupBy("product_id", "name", "category")
                          .agg(F.sum("quantity").alias("units"),
                               F.round(F.sum("amount"), 2).alias("revenue"),
                               F.countDistinct("order_id").alias("orders"))
                          .orderBy(F.desc("units"), F.desc("revenue")))
    save_mart(top_products_sales, "top_products_sales", "Самые продаваемые товары")

    # --- Рекомендации ---
    popularity = interactions.groupBy("product_id").agg(F.sum("weight").alias("views"))
    catalog = (products.join(popularity, "product_id", "left")
               .fillna(0, subset=["views"])
               .filter(F.coalesce(F.col("stock.available"), F.lit(0)) > 0)
               .select("product_id", "name", "category", "brand",
                       F.col("price.amount").alias("price"),
                       F.col("price.currency").alias("currency"), "views"))

    user_categories = (interactions.join(product_categories, "product_id")
                       .groupBy("user_id", "category")
                       .agg(F.sum("weight").alias("affinity")))
    already_bought = purchases.select("user_id", "product_id").distinct()

    by_score = Window.partitionBy("user_id").orderBy(F.desc("score"), F.desc("views"), "product_id")
    personal = (user_categories.join(catalog, "category")
                .join(already_bought, ["user_id", "product_id"], "left_anti")
                .withColumn("score", (F.col("affinity") * 10 + F.col("views")).cast("double"))
                .withColumn("rank", F.row_number().over(by_score))
                .filter(F.col("rank") <= TOP_N)
                .withColumn("strategy", F.lit("category_affinity")))

    # Холодный старт: пользователи без поисков и покупок получают популярные товары
    all_users = events.select("user_id").unionByName(order_lines.select("user_id")).distinct()
    cold_users = all_users.join(user_categories.select("user_id").distinct(), "user_id", "left_anti")
    popular = (catalog.withColumn("score", F.col("views").cast("double"))
               .withColumn("rank", F.row_number().over(
                   Window.orderBy(F.desc("views"), F.desc("price"), "product_id")))
               .filter(F.col("rank") <= TOP_N))
    cold = (cold_users.crossJoin(popular)
            .withColumn("affinity", F.lit(0).cast("long"))
            .withColumn("strategy", F.lit("popular")))

    columns = ["user_id", "strategy", "rank", "product_id", "name", "category",
               "brand", "price", "currency", "score"]
    ranked = personal.select(*columns).unionByName(cold.select(*columns))

    interests = (user_categories
                 .withColumn("r", F.row_number().over(
                     Window.partitionBy("user_id").orderBy(F.desc("affinity"), "category")))
                 .filter("r <= 3")
                 .groupBy("user_id").agg(F.sort_array(F.collect_list(
                     F.struct(F.col("r"), F.col("category")))).alias("c"))
                 .select("user_id", F.col("c.category").alias("based_on_categories")))

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    recommendations = (ranked.groupBy("user_id", "strategy")
                       .agg(F.sort_array(F.collect_list(F.struct(
                           "rank", "product_id", "name", "category", "brand",
                           "price", "currency", "score"))).alias("recommendations"))
                       .join(interests, "user_id", "left")
                       .withColumn("based_on_categories",
                                   F.coalesce("based_on_categories",
                                              F.array().cast("array<string>")))
                       .withColumn("generated_at", F.lit(generated_at))
                       .cache())

    flat_view = ranked.orderBy("user_id", "rank")
    save_mart(recommendations, "recommendations", "Рекомендации по пользователям")
    print("\n=== Рекомендации (построчно)")
    flat_view.show(50, truncate=False)

    total = recommendations.count()
    if total == 0:
        print("[analytics] Нет событий клиентов — рекомендации не сформированы. "
              "Выполните несколько команд client-api search/recommend.")
        spark.stop()
        return

    out = recommendations.select(
        F.col("user_id").alias("key"),
        F.to_json(F.struct("user_id", "strategy", "based_on_categories",
                           "recommendations", "generated_at")).alias("value"))
    (out.write.format("kafka")
     .options(**kafka_ssl_options())
     .option("topic", RECOMMENDATIONS_TOPIC)
     .save())
    print(f"\n[analytics] Записано {total} рекомендаций в топик {RECOMMENDATIONS_TOPIC} "
          f"основного кластера ({PRIMARY_BOOTSTRAP})")
    spark.stop()


if __name__ == "__main__":
    main()
