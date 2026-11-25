"""Эмуляция SHOP API: магазины отправляют товары в Kafka.

Товары читаются из JSON-файла (data/products.json, хранится в Git) и отправляются
в топик shop-products с ключом product_id.

    python -m marketplace.shop_api send
    python -m marketplace.shop_api send --file data/products.json --delay 0.2
    python -m marketplace.shop_api send --inject-bad 2   # + битые сообщения (проверка DLQ)
"""
import argparse
import json
import logging
import sys
import time
from typing import Any

from confluent_kafka import KafkaException

from marketplace.config import TOPIC_SHOP_PRODUCTS, setup_logging
from marketplace.kafka_client import DeliveryTracker, make_producer

log = logging.getLogger("shop-api")

IDENTITY = "shop-api"
REQUIRED_FIELDS = ("product_id", "name", "price", "category", "store_id")


def load_products(path: str) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path}: ожидается JSON-массив товаров")
    return data


def validate(product: Any) -> list[str]:
    """Возвращает список ошибок (пустой список — товар корректен)."""
    if not isinstance(product, dict):
        return ["товар должен быть JSON-объектом"]
    errors = [f"нет поля {f}" for f in REQUIRED_FIELDS if f not in product]
    price = product.get("price")
    if isinstance(price, dict):
        if not isinstance(price.get("amount"), (int, float)):
            errors.append("price.amount должен быть числом")
    elif "price" in product:
        errors.append("price должен быть объектом {amount, currency}")
    return errors


def cmd_send(args: argparse.Namespace) -> int:
    products = load_products(args.file)
    producer = make_producer(IDENTITY)
    tracker = DeliveryTracker()
    sent, skipped, not_sent = 0, 0, 0

    log.info("Отправка %d товаров из %s в топик %s", len(products), args.file, TOPIC_SHOP_PRODUCTS)
    for product in products:
        errors = validate(product)
        if errors:
            skipped += 1
            log.warning("Товар пропущен (%s): %s", "; ".join(errors), str(product)[:120])
            continue

        product_id = str(product["product_id"])
        payload = json.dumps(product, ensure_ascii=False).encode("utf-8")
        try:
            producer.produce(TOPIC_SHOP_PRODUCTS, key=product_id.encode(), value=payload,
                             on_delivery=tracker)
            sent += 1
            log.info("-> %s | %s | магазин %s", product_id, product["name"], product["store_id"])
        except (BufferError, KafkaException) as exc:
            not_sent += 1
            log.error("Не удалось поставить в очередь товар %s: %s", product_id, exc)
        producer.poll(0)
        if args.delay:
            time.sleep(args.delay)

    # Битые сообщения: не JSON. Потоковый обработчик должен отправить их в DLQ, а не упасть.
    for i in range(args.inject_bad):
        try:
            producer.produce(TOPIC_SHOP_PRODUCTS, key=f"broken-{i + 1}".encode(),
                             value=b"{not a valid json", on_delivery=tracker)
            sent += 1
            log.warning("-> broken-%d | намеренно битое сообщение", i + 1)
        except (BufferError, KafkaException) as exc:
            not_sent += 1
            log.error("Не удалось поставить в очередь битое сообщение: %s", exc)

    remaining = producer.flush(30)
    not_delivered = tracker.failed + remaining + not_sent
    log.info("Итог: поставлено в очередь=%d, подтверждено брокером=%d, не доставлено=%d, "
             "пропущено при валидации=%d", sent, tracker.delivered, not_delivered, skipped)
    return 1 if not_delivered else 0


def main() -> int:
    setup_logging()
    parser = argparse.ArgumentParser(prog="shop-api", description="Эмуляция SHOP API")
    sub = parser.add_subparsers(dest="command", required=True)

    send = sub.add_parser("send", help="отправить товары из файла в Kafka")
    send.add_argument("--file", default="data/products.json", help="JSON-файл с товарами")
    send.add_argument("--delay", type=float, default=0.0, help="пауза между сообщениями, сек")
    send.add_argument("--inject-bad", type=int, default=0, metavar="N",
                      help="дополнительно отправить N битых сообщений (проверка DLQ)")
    send.set_defaults(func=cmd_send)

    args = parser.parse_args()
    try:
        return args.func(args)
    except (OSError, ValueError) as exc:
        log.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
