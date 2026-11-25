"""Общие настройки сервисов. Все значения можно переопределить переменными окружения."""
import logging
import os

# Основной кластер Kafka: все три брокера, адреса внутри docker-сети (listener INTERNAL_SSL)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:19092,kafka-2:19092,kafka-3:19092")

# Каталог с сертификатами: ca.crt и подкаталоги <identity>/<identity>.crt|.key
CERTS_DIR = os.getenv("CERTS_DIR", "/certs")

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200").rstrip("/")
# Пользователь Elasticsearch с правами только на чтение (создаётся scripts/init-elasticsearch.sh)
ES_USER = os.getenv("ES_USER", "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")

# --- Топики основного кластера ---
TOPIC_SHOP_PRODUCTS = "shop-products"          # сырые товары от магазинов (SHOP API)
TOPIC_SHOP_PRODUCTS_DLQ = "shop-products-dlq"  # сообщения, которые не удалось разобрать
TOPIC_PRODUCTS_FILTERED = "products-filtered"  # товары после фильтрации запрещённых
TOPIC_BANNED_PRODUCTS = "banned-products"      # команды управления списком запрещённых (compact)
TOPIC_CLIENT_REQUESTS = "client-requests"      # запросы клиентов (CLIENT API)
TOPIC_ORDERS = "orders"                        # заказы клиентов (CLIENT API buy)
TOPIC_RECOMMENDATIONS = "recommendations"      # рекомендации от Spark (compact)

# --- Индексы Elasticsearch (имя индекса = имя топика, так пишет Elasticsearch Sink) ---
INDEX_PRODUCTS = TOPIC_PRODUCTS_FILTERED
INDEX_CLIENT_REQUESTS = TOPIC_CLIENT_REQUESTS
INDEX_RECOMMENDATIONS = TOPIC_RECOMMENDATIONS
INDEX_ORDERS = TOPIC_ORDERS

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def setup_logging() -> None:
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
