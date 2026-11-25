"""Эмуляция CLIENT API: команды клиента маркетплейса в терминале.

    python -m marketplace.client_api search "умные часы" --user-id user_001
    python -m marketplace.client_api recommend --user-id user_001
    python -m marketplace.client_api history --user-id user_001
    python -m marketplace.client_api buy --user-id user_001 --product-id 12345 --quantity 1
    python -m marketplace.client_api orders --user-id user_001

Каждый запрос:
  1) выполняется по системе хранения и поиска (Elasticsearch, индексы наполняет Kafka Connect);
  2) публикуется событием в топик client-requests основного кластера Kafka для аналитики.
     Из Kafka событие также попадает в Elasticsearch (индекс client-requests) через Connect.

Заказ (buy) проверяет товар по хранилищу (там только разрешённые товары) и публикуется
в топик orders. Из него заказ попадает в Elasticsearch (индекс orders) и в аналитику.
"""
import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from confluent_kafka import KafkaException

from marketplace.config import (
    ES_PASSWORD,
    ES_URL,
    ES_USER,
    INDEX_CLIENT_REQUESTS,
    INDEX_ORDERS,
    INDEX_PRODUCTS,
    INDEX_RECOMMENDATIONS,
    TOPIC_CLIENT_REQUESTS,
    TOPIC_ORDERS,
    setup_logging,
)
from marketplace.kafka_client import DeliveryTracker, make_producer

log = logging.getLogger("client-api")

IDENTITY = "client-api"
HTTP_TIMEOUT = 10


class StorageError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Elasticsearch
# ---------------------------------------------------------------------------
def es_request(method: str, path: str, body: Optional[dict] = None) -> Optional[dict]:
    """Запрос к Elasticsearch. Возвращает JSON или None, если индекс/документ не найден."""
    try:
        resp = requests.request(method, f"{ES_URL}/{path}", json=body, timeout=HTTP_TIMEOUT,
                                auth=(ES_USER, ES_PASSWORD) if ES_USER else None)
    except requests.RequestException as exc:
        raise StorageError(f"Elasticsearch недоступен ({ES_URL}): {exc}") from exc
    if resp.status_code == 404:
        return None
    if not resp.ok:
        raise StorageError(f"Elasticsearch вернул {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def search_products(name: str, size: int) -> list[dict[str, Any]]:
    query = {
        "size": size,
        "query": {
            "bool": {
                "should": [
                    # точное совпадение фразы в названии — выше в выдаче
                    {"match_phrase": {"name": {"query": name, "boost": 3}}},
                    # все слова запроса с допуском опечаток
                    {"match": {"name": {"query": name, "operator": "and", "fuzziness": "AUTO"}}},
                ],
                "minimum_should_match": 1,
            }
        },
    }
    result = es_request("POST", f"{INDEX_PRODUCTS}/_search", query)
    if result is None:
        raise StorageError(f"Индекс {INDEX_PRODUCTS} не найден: товары ещё не загружены "
                           f"(запустите shop-api send и проверьте коннектор)")
    return [hit["_source"] for hit in result["hits"]["hits"]]


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def publish(topic: str, key: str, value: dict) -> bool:
    """Синхронная отправка одного сообщения: True, если брокер подтвердил запись (acks=all)."""
    producer = make_producer(IDENTITY)
    tracker = DeliveryTracker()
    try:
        producer.produce(topic, key=key.encode(),
                         value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
                         on_delivery=tracker)
    except (BufferError, KafkaException) as exc:
        log.error("Сообщение %s не отправлено в топик %s: %s", key, topic, exc)
        return False
    remaining = producer.flush(15)
    if tracker.delivered != 1 or remaining:
        log.error("Сообщение %s не подтверждено брокером (топик %s)", key, topic)
        return False
    return True


def publish_event(event_type: str, user_id: str, query: Optional[str] = None,
                  result_ids: Optional[list[str]] = None) -> bool:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "query": query,
        "result_product_ids": result_ids or [],
        "result_count": len(result_ids or []),
        "created_at": now_iso(),
    }
    ok = publish(TOPIC_CLIENT_REQUESTS, event["event_id"], event)
    if ok:
        log.info("Событие %s (%s) записано в топик %s", event["event_id"], event_type,
                 TOPIC_CLIENT_REQUESTS)
    return ok


def get_product(product_id: str) -> Optional[dict[str, Any]]:
    """Товар из хранилища. В индексе есть только разрешённые товары, поэтому купить
    запрещённый товар нельзя: его там нет."""
    doc = es_request("GET", f"{INDEX_PRODUCTS}/_doc/{product_id}")
    return doc["_source"] if doc and doc.get("found") else None


# ---------------------------------------------------------------------------
# Вывод
# ---------------------------------------------------------------------------
def fmt_price(price: Any) -> str:
    if isinstance(price, dict):
        return f"{price.get('amount', '?')} {price.get('currency', '')}".strip()
    return str(price)


def print_products(products: list[dict[str, Any]]) -> None:
    for i, p in enumerate(products, 1):
        stock = p.get("stock") or {}
        print(f"{i:>2}. [{p.get('product_id')}] {p.get('name')}")
        print(f"    {p.get('category')} | {p.get('brand')} | {fmt_price(p.get('price'))} | "
              f"в наличии: {stock.get('available', '?')} | магазин: {p.get('store_id')}")
        if p.get("description"):
            print(f"    {p['description']}")


# ---------------------------------------------------------------------------
# Команды
# ---------------------------------------------------------------------------
def cmd_search(args: argparse.Namespace) -> int:
    products = search_products(args.name, args.size)
    print(f"\nПоиск «{args.name}»: найдено {len(products)}")
    print_products(products)
    ok = publish_event("search", args.user_id, query=args.name,
                       result_ids=[str(p.get("product_id")) for p in products])
    return 0 if ok else 1


def cmd_recommend(args: argparse.Namespace) -> int:
    doc = es_request("GET", f"{INDEX_RECOMMENDATIONS}/_doc/{args.user_id}")
    ok = publish_event("recommendations", args.user_id)
    if not doc or not doc.get("found"):
        print(f"\nДля пользователя {args.user_id} рекомендаций пока нет.")
        print("Они появятся после запуска аналитики: bash scripts/run-analytics.sh")
        return 0 if ok else 1

    rec = doc["_source"]
    print(f"\nРекомендации для {args.user_id} (стратегия: {rec.get('strategy')}, "
          f"рассчитаны {rec.get('generated_at')})")
    if rec.get("based_on_categories"):
        print(f"Интересы пользователя: {', '.join(rec['based_on_categories'])}")
    items = rec.get("recommendations", [])
    # Рекомендации считаются пакетно, а товар могли запретить позже: показываем только те,
    # что сейчас есть в хранилище (запрещённые удаляются оттуда потоковым фильтром).
    available = available_product_ids([str(i.get("product_id")) for i in items])
    hidden = 0
    for item in items:
        if str(item.get("product_id")) not in available:
            hidden += 1
            continue
        print(f"{item.get('rank'):>2}. [{item.get('product_id')}] {item.get('name')} | "
              f"{item.get('category')} | {item.get('price')} {item.get('currency') or ''} | "
              f"score={item.get('score')}")
    if hidden:
        print(f"(скрыто недоступных для продажи товаров: {hidden})")
    return 0 if ok else 1


def available_product_ids(product_ids: list[str]) -> set[str]:
    if not product_ids:
        return set()
    result = es_request("POST", f"{INDEX_PRODUCTS}/_mget", {"ids": product_ids})
    docs = result.get("docs", []) if result else []
    return {d["_id"] for d in docs if d.get("found")}


def cmd_buy(args: argparse.Namespace) -> int:
    if args.quantity < 1:
        log.error("Количество должно быть не меньше 1")
        return 2
    product = get_product(args.product_id)
    if product is None:
        print(f"\nТовар {args.product_id} не найден или недоступен для продажи.")
        return 1

    price = product.get("price") or {}
    amount = float(price.get("amount") or 0)
    available = (product.get("stock") or {}).get("available")
    if isinstance(available, int) and available < args.quantity:
        print(f"\nНедостаточно товара на складе: доступно {available}, запрошено {args.quantity}.")
        return 1

    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": args.user_id,
        "status": "created",
        "items": [{
            "product_id": str(product["product_id"]),
            "name": product.get("name"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "store_id": product.get("store_id"),
            "quantity": args.quantity,
            "price": amount,
            "currency": price.get("currency", "RUB"),
        }],
        "total_amount": round(amount * args.quantity, 2),
        "currency": price.get("currency", "RUB"),
        "created_at": now_iso(),
    }
    if not publish(TOPIC_ORDERS, order["order_id"], order):
        print("\nЗаказ не оформлен: Kafka не подтвердила запись.")
        return 1
    log.info("Заказ %s записан в топик %s", order["order_id"], TOPIC_ORDERS)
    print(f"\nЗаказ {order['order_id']} оформлен")
    print(f"  {product.get('name')} × {args.quantity} = {order['total_amount']} {order['currency']}")
    return 0


def cmd_orders(args: argparse.Namespace) -> int:
    query = {
        "size": args.size,
        "sort": [{"created_at": "desc"}],
        "query": {"term": {"user_id": args.user_id}},
    }
    result = es_request("POST", f"{INDEX_ORDERS}/_search", query)
    hits = result["hits"]["hits"] if result else []
    print(f"\nЗаказы пользователя {args.user_id} (из Elasticsearch): {len(hits)}")
    for hit in hits:
        o = hit["_source"]
        items = ", ".join(f"{i.get('name')} × {i.get('quantity')}" for i in o.get("items", []))
        print(f"  {o.get('created_at')} | {o.get('order_id')} | {o.get('status')} | "
              f"{o.get('total_amount')} {o.get('currency')} | {items}")
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    query = {
        "size": args.size,
        "sort": [{"created_at": "desc"}],
        "query": {"term": {"user_id": args.user_id}},
    }
    result = es_request("POST", f"{INDEX_CLIENT_REQUESTS}/_search", query)
    hits = result["hits"]["hits"] if result else []
    print(f"\nПоследние запросы пользователя {args.user_id} (из Elasticsearch): {len(hits)}")
    for hit in hits:
        e = hit["_source"]
        print(f"  {e.get('created_at')} | {e.get('event_type'):<15} | "
              f"{e.get('query') or '-'} | найдено: {e.get('result_count')}")
    return 0


def main() -> int:
    setup_logging()
    parser = argparse.ArgumentParser(prog="client-api", description="Эмуляция CLIENT API")
    sub = parser.add_subparsers(dest="command", required=True)

    search = sub.add_parser("search", help="поиск товара по названию")
    search.add_argument("name", help="название товара или его часть")
    search.add_argument("--user-id", default="anonymous")
    search.add_argument("--size", type=int, default=5)
    search.set_defaults(func=cmd_search)

    recommend = sub.add_parser("recommend", help="персонализированные рекомендации")
    recommend.add_argument("--user-id", required=True)
    recommend.set_defaults(func=cmd_recommend)

    buy = sub.add_parser("buy", help="оформить заказ (покупка товара)")
    buy.add_argument("--user-id", required=True)
    buy.add_argument("--product-id", required=True)
    buy.add_argument("--quantity", type=int, default=1)
    buy.set_defaults(func=cmd_buy)

    orders = sub.add_parser("orders", help="заказы пользователя")
    orders.add_argument("--user-id", required=True)
    orders.add_argument("--size", type=int, default=10)
    orders.set_defaults(func=cmd_orders)

    history = sub.add_parser("history", help="история запросов пользователя")
    history.add_argument("--user-id", required=True)
    history.add_argument("--size", type=int, default=10)
    history.set_defaults(func=cmd_history)

    args = parser.parse_args()
    try:
        return args.func(args)
    except (StorageError, FileNotFoundError) as exc:
        log.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
