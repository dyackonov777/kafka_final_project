"""Потоковая обработка товаров (Faust): фильтр запрещённых товаров и учёт резервов по заказам.

  shop-products ───► [moderate_products] ──► products-filtered (разрешённые товары с учётом резервов)
                          │      ▲      └──► shop-products-dlq (нечитаемые сообщения)
                          │      │ lookup
  banned-products ─► [apply_ban_commands] ─► таблица banned-products-table
                          └──► tombstone в products-filtered: товар удаляется из хранилища
  orders ──► [split_orders] ──► faust-moderation-stock-reservations (ключ product_id)
                                   └──► [apply_reservations] ─► таблица stock-reserved
                                          └──► products-filtered (обновлённый остаток)

Модель остатка: магазин присылает фактический остаток (stock.available), в котором уже учтены
продажи. Платформа держит резерв по заказам, оформленным ПОСЛЕ последней версии товара от магазина:
  опубликованный available = available магазина − резерв,  reserved = reserved магазина + резерв.
Новая версия товара от магазина обнуляет резерв — повторного вычитания тех же заказов нет.

Запрещённые товары дальше не передаются. Если товар запретили после того, как он попал
в хранилище, фильтр отправляет в products-filtered tombstone (ключ product_id, значение null):
Elasticsearch Sink удаляет документ, Spark исключает товар из каталога. При снятии запрета
последняя версия товара публикуется снова.

Партиционирование: shop-products, banned-products, products-filtered и топик резервов имеют
ключ product_id, 3 партиции и одинаковый хеш (murmur2). Команда запрета, товар и резерв
попадают в партицию с одним номером, а Faust назначает такие партиции одному воркеру
(co-partitioning). Поэтому таблицы шардируются так же, как топики, и обработчик можно
масштабировать на несколько экземпляров. Заказы (ключ order_id) сначала перекладываются
в топик резервов с ключом product_id (repartition).

Метрики Prometheus: http://faust-moderation:6066/metrics
Запуск: python -m marketplace.moderation worker -l info
"""
import json
import logging
import os
import ssl
from datetime import datetime, timezone
from typing import Optional

import faust
from faust.sensors.prometheus import setup_prometheus_sensors
from prometheus_client import Counter

from marketplace.config import (
    CERTS_DIR,
    KAFKA_BOOTSTRAP,
    TOPIC_BANNED_PRODUCTS,
    TOPIC_ORDERS,
    TOPIC_PRODUCTS_FILTERED,
    TOPIC_SHOP_PRODUCTS,
    TOPIC_SHOP_PRODUCTS_DLQ,
)

log = logging.getLogger("moderation")

APP_ID = "faust-moderation"
IDENTITY = "moderation"
PARTITIONS = 3


def build_ssl_context() -> ssl.SSLContext:
    """mTLS: проверяем сертификат брокера по нашему CA и предъявляем свой (CN=moderation)."""
    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH,
                                     cafile=os.path.join(CERTS_DIR, "ca.crt"))
    ctx.load_cert_chain(certfile=os.path.join(CERTS_DIR, IDENTITY, f"{IDENTITY}.crt"),
                        keyfile=os.path.join(CERTS_DIR, IDENTITY, f"{IDENTITY}.key"))
    return ctx


def broker_url() -> str:
    # Faust ожидает формат kafka://host1:port;host2:port;host3:port — указываем все брокеры
    hosts = [h.strip() for h in KAFKA_BOOTSTRAP.split(",") if h.strip()]
    return "kafka://" + ";".join(hosts)


app = faust.App(
    APP_ID,
    broker=broker_url(),
    broker_credentials=faust.SSLCredentials(build_ssl_context()),
    # Состояние таблиц хранится в памяти и восстанавливается из changelog-топиков при старте
    store="memory://",
    topic_partitions=PARTITIONS,
    topic_replication_factor=3,
    producer_acks=-1,
    web_port=6066,
)

# Стандартные метрики Faust (faust_moderation_*) + метрики решений фильтра на /metrics
setup_prometheus_sensors(app)
DECISIONS = Counter("marketplace_moderation_decisions_total",
                    "Решения потокового фильтра по товарам", ["decision"])
RESERVATIONS = Counter("marketplace_stock_reserved_units_total",
                       "Зарезервировано единиц товара по заказам")

# Сообщения передаются «как есть» (raw bytes): JSON разбираем сами, чтобы битое
# сообщение не роняло агента на этапе десериализации.
RAW = {"key_serializer": "raw", "value_serializer": "raw", "partitions": PARTITIONS}
shop_products_topic = app.topic(TOPIC_SHOP_PRODUCTS, **RAW)
banned_topic = app.topic(TOPIC_BANNED_PRODUCTS, **RAW)
filtered_topic = app.topic(TOPIC_PRODUCTS_FILTERED, **RAW)
dlq_topic = app.topic(TOPIC_SHOP_PRODUCTS_DLQ, **RAW)
orders_topic = app.topic(TOPIC_ORDERS, **RAW)
# Внутренний топик Faust: позиции заказов, перекладываемые по ключу product_id
reservations_topic = app.topic(f"{APP_ID}-stock-reservations", internal=True, **RAW)

# product_id -> {"reason": ..., "updated_at": ...}
banned_table = app.Table("banned-products-table", default=None, partitions=PARTITIONS, key_type=str)
# product_id -> последняя разрешённая версия товара от магазина (как прислал магазин)
products_table = app.Table("products-latest", default=None, partitions=PARTITIONS, key_type=str)
# product_id -> сколько единиц зарезервировано заказами
reserved_table = app.Table("stock-reserved", default=int, partitions=PARTITIONS, key_type=str)
# order_id -> True: защита от повторной обработки заказа (at-least-once доставка)
processed_orders = app.Table("orders-processed", default=None, partitions=PARTITIONS, key_type=str)

stats = {"passed": 0, "blocked": 0, "invalid": 0, "removed": 0, "reserved_units": 0}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def decode_key(key) -> str:
    return key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else str(key)


def with_stock(product: dict) -> dict:
    """Товар с учётом резервов: available уменьшается, reserved растёт на число заказанных единиц."""
    product_id = str(product["product_id"])
    reserved = int(reserved_table[product_id] or 0)
    if not reserved:
        return product
    stock = dict(product.get("stock") or {})
    stock["available"] = max(0, int(stock.get("available") or 0) - reserved)
    stock["reserved"] = int(stock.get("reserved") or 0) + reserved
    return {**product, "stock": stock}


async def publish_product(product: dict) -> None:
    product_id = str(product["product_id"])
    body = json.dumps(with_stock(product), ensure_ascii=False).encode("utf-8")
    await filtered_topic.send(key=product_id.encode("utf-8"), value=body)


async def remove_from_storage(product_id: str) -> None:
    """Tombstone в products-filtered: Elasticsearch Sink удаляет документ, Spark — товар из каталога."""
    await filtered_topic.send(key=product_id.encode("utf-8"), value=None)


# ---------------------------------------------------------------------------
# Список запрещённых товаров
# ---------------------------------------------------------------------------
@app.agent(banned_topic)
async def apply_ban_commands(stream):
    """Применяет команды add/remove к таблице запрещённых товаров."""
    async for event in stream.events():
        try:
            if event.value is None:  # tombstone в compacted-топике
                continue
            cmd = json.loads(event.value)
            product_id = str(cmd["product_id"]).strip()
            action = str(cmd.get("action", "")).lower()
            if not product_id:
                raise ValueError("пустой product_id")

            if action == "add":
                banned_table[product_id] = {"reason": cmd.get("reason", ""),
                                            "updated_at": cmd.get("updated_at", now())}
                # Товар мог попасть в хранилище до запрета — удаляем его оттуда
                await remove_from_storage(product_id)
                stats["removed"] += 1
                DECISIONS.labels("removed").inc()
                log.info("[ban] товар %s добавлен в список запрещённых (%s) и удалён из хранилища",
                         product_id, cmd.get("reason", ""))
            elif action == "remove":
                if product_id in banned_table:
                    del banned_table[product_id]
                # Если товар уже присылали, публикуем его снова — он вернётся в хранилище
                latest: Optional[dict] = products_table.get(product_id)
                if latest is not None:
                    await publish_product(latest)
                log.info("[ban] товар %s удалён из списка запрещённых%s", product_id,
                         ", последняя версия опубликована" if latest is not None else "")
            else:
                log.warning("[ban] неизвестное действие %r для товара %s", action, product_id)
        except (ValueError, KeyError, TypeError) as exc:
            log.error("[ban] некорректная команда key=%s: %s", decode_key(event.key), exc)
        except Exception:  # noqa: BLE001 — агент не должен падать из-за одной команды
            log.exception("[ban] ошибка обработки команды key=%s", decode_key(event.key))


# ---------------------------------------------------------------------------
# Фильтр товаров магазинов
# ---------------------------------------------------------------------------
async def send_to_dlq(key, raw, error: str) -> None:
    body = {
        "error": error,
        "source_topic": TOPIC_SHOP_PRODUCTS,
        "raw_value": raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else raw,
        "failed_at": now(),
    }
    await dlq_topic.send(key=key, value=json.dumps(body, ensure_ascii=False).encode("utf-8"))


@app.agent(shop_products_topic)
async def moderate_products(stream):
    """Пропускает дальше только разрешённые товары."""
    async for event in stream.events():
        key, raw = event.key, event.value
        try:
            if raw is None:
                continue
            product = json.loads(raw)
            if not isinstance(product, dict):
                raise ValueError("ожидается JSON-объект")
            product_id = str(product["product_id"]).strip()
            if not product_id:
                raise ValueError("пустой product_id")

            ban = banned_table.get(product_id)
            if ban is not None:
                stats["blocked"] += 1
                DECISIONS.labels("blocked").inc()
                log.warning("[BLOCKED] %s | %s | магазин %s | причина: %s",
                            product_id, product.get("name"), product.get("store_id"),
                            ban.get("reason") or "-")
                continue

            # Новая версия товара от магазина — источник истины по остатку: магазин уже учёл
            # проданное. Резерв платформы обнуляется и дальше копится только по новым заказам,
            # иначе одни и те же заказы вычитались бы из остатка повторно.
            previous_reserve = int(reserved_table[product_id] or 0)
            if product_id in reserved_table:
                del reserved_table[product_id]
            if previous_reserve:
                log.info("[stock] товар %s: магазин прислал новый остаток, резерв %d сброшен",
                         product_id, previous_reserve)

            products_table[product_id] = product
            await publish_product(product)
            stats["passed"] += 1
            DECISIONS.labels("passed").inc()
            log.info("[PASSED]  %s | %s", product_id, product.get("name"))
        except (ValueError, KeyError, TypeError) as exc:
            stats["invalid"] += 1
            DECISIONS.labels("invalid").inc()
            log.error("[INVALID] key=%s: %s -> %s", decode_key(key), exc, TOPIC_SHOP_PRODUCTS_DLQ)
            try:
                await send_to_dlq(key, raw, str(exc))
            except Exception:  # noqa: BLE001
                log.exception("Не удалось записать сообщение key=%s в DLQ", decode_key(key))
        except Exception:  # noqa: BLE001 — поток продолжает работу при любой ошибке
            stats["invalid"] += 1
            DECISIONS.labels("error").inc()
            log.exception("[ERROR] key=%s: ошибка обработки товара", decode_key(key))


# ---------------------------------------------------------------------------
# Резервы по заказам
# ---------------------------------------------------------------------------
@app.agent(orders_topic)
async def split_orders(stream):
    """Раскладывает позиции заказа по товарам (ключ product_id) для учёта резервов."""
    async for event in stream.events():
        try:
            if event.value is None:
                continue
            order = json.loads(event.value)
            order_id = str(order["order_id"])
            if processed_orders.get(order_id):
                log.info("[order] заказ %s уже учтён, повтор пропущен", order_id)
                continue
            processed_orders[order_id] = True
            for item in order.get("items", []):
                product_id = str(item["product_id"])
                quantity = int(item.get("quantity") or 1)
                await reservations_topic.send(
                    key=product_id.encode("utf-8"),
                    value=json.dumps({"order_id": order_id, "quantity": quantity}).encode("utf-8"))
        except (ValueError, KeyError, TypeError) as exc:
            log.error("[order] некорректный заказ key=%s: %s", decode_key(event.key), exc)
        except Exception:  # noqa: BLE001
            log.exception("[order] ошибка обработки заказа key=%s", decode_key(event.key))


@app.agent(reservations_topic)
async def apply_reservations(stream):
    """Увеличивает резерв товара и публикует товар с обновлённым остатком."""
    async for event in stream.events():
        try:
            product_id = decode_key(event.key)
            reservation = json.loads(event.value)
            quantity = int(reservation["quantity"])
            reserved_table[product_id] = int(reserved_table[product_id] or 0) + quantity
            stats["reserved_units"] += quantity
            RESERVATIONS.inc(quantity)

            latest: Optional[dict] = products_table.get(product_id)
            if latest is not None and banned_table.get(product_id) is None:
                await publish_product(latest)
            log.info("[stock] товар %s: +%d в резерв по заказу %s (всего в резерве %d)",
                     product_id, quantity, reservation.get("order_id"), reserved_table[product_id])
        except (ValueError, KeyError, TypeError) as exc:
            log.error("[stock] некорректный резерв key=%s: %s", decode_key(event.key), exc)
        except Exception:  # noqa: BLE001
            log.exception("[stock] ошибка учёта резерва key=%s", decode_key(event.key))


@app.timer(interval=60.0)
async def report_stats():
    log.info("Статистика: пропущено=%d, заблокировано=%d, удалено из хранилища=%d, "
             "некорректных=%d, зарезервировано единиц=%d",
             stats["passed"], stats["blocked"], stats["removed"], stats["invalid"],
             stats["reserved_units"])


if __name__ == "__main__":
    app.main()
