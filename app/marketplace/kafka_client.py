"""Подключение к основному кластеру Kafka по mTLS.

Каждый сервис использует свой клиентский сертификат (identity). CN сертификата становится
principal'ом User:CN=<identity>, и брокер проверяет его права по ACL.
"""
import logging
import os
from typing import Optional

from confluent_kafka import Consumer, KafkaError, Message, Producer

from marketplace.config import CERTS_DIR, KAFKA_BOOTSTRAP

log = logging.getLogger(__name__)


def ssl_config(identity: str) -> dict:
    """Параметры librdkafka для подключения к кластеру с сертификатом identity."""
    cert_dir = os.path.join(CERTS_DIR, identity)
    paths = {
        "ssl.ca.location": os.path.join(CERTS_DIR, "ca.crt"),
        "ssl.certificate.location": os.path.join(cert_dir, f"{identity}.crt"),
        "ssl.key.location": os.path.join(cert_dir, f"{identity}.key"),
    }
    missing = [p for p in paths.values() if not os.path.isfile(p)]
    if missing:
        raise FileNotFoundError(f"Не найдены файлы сертификатов: {', '.join(missing)}")
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": "SSL",
        # Проверяем, что имя брокера совпадает с SAN его сертификата
        "ssl.endpoint.identification.algorithm": "https",
        "client.id": identity,
        **paths,
    }


def make_producer(identity: str, **overrides) -> Producer:
    config = {
        **ssl_config(identity),
        # Надёжная запись: подтверждение от всех in-sync реплик (min.insync.replicas=2)
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "delivery.timeout.ms": 30000,
        # Тот же алгоритм партиционирования, что у Java-клиентов и Faust (aiokafka):
        # одинаковый ключ product_id попадает в одинаковую партицию во всех топиках.
        "partitioner": "murmur2_random",
        **overrides,
    }
    return Producer(config)


def make_consumer(identity: str, group_id: str, **overrides) -> Consumer:
    config = {
        **ssl_config(identity),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        **overrides,
    }
    return Consumer(config)


class DeliveryTracker:
    """Считает подтверждённые и неудачные доставки сообщений продюсера."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def __call__(self, err: Optional[KafkaError], msg: Message) -> None:
        key = msg.key().decode("utf-8", "replace") if msg.key() else None
        if err is not None:
            self.failed += 1
            log.error("Не доставлено: topic=%s key=%s ошибка=%s", msg.topic(), key, err)
        else:
            self.delivered += 1
            log.debug("Доставлено: topic=%s partition=%s offset=%s key=%s",
                      msg.topic(), msg.partition(), msg.offset(), key)
