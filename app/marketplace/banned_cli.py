"""CLI управления списком товаров, запрещённых к продаже.

Список хранится в Kafka: каждая команда — сообщение в compacted-топике banned-products
с ключом product_id. Faust-приложение читает топик и ведёт по нему таблицу (state store),
поэтому изменения применяются на лету, без перезапуска обработчика.

    python -m marketplace.banned_cli add --product-id 90001 --reason "Табачная продукция"
    python -m marketplace.banned_cli remove --product-id 90001
    python -m marketplace.banned_cli list
    python -m marketplace.banned_cli load --file data/banned-products.json
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from confluent_kafka import KafkaError, KafkaException, TopicPartition, OFFSET_BEGINNING

from marketplace.config import TOPIC_BANNED_PRODUCTS, setup_logging
from marketplace.kafka_client import DeliveryTracker, make_consumer, make_producer

log = logging.getLogger("banned-products")

IDENTITY = "moderator"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def send_commands(commands: list[dict]) -> int:
    producer = make_producer(IDENTITY)
    tracker = DeliveryTracker()
    for cmd in commands:
        producer.produce(TOPIC_BANNED_PRODUCTS, key=cmd["product_id"].encode(),
                         value=json.dumps(cmd, ensure_ascii=False).encode("utf-8"),
                         on_delivery=tracker)
    remaining = producer.flush(30)
    failed = tracker.failed + remaining
    for cmd in commands:
        verb = "добавлен в список" if cmd["action"] == "add" else "удалён из списка"
        log.info("Товар %s %s запрещённых%s", cmd["product_id"], verb,
                 f" (причина: {cmd['reason']})" if cmd.get("reason") else "")
    if failed:
        log.error("Не доставлено команд: %d", failed)
    return 1 if failed else 0


def cmd_add(args: argparse.Namespace) -> int:
    return send_commands([{"action": "add", "product_id": args.product_id.strip(),
                           "reason": args.reason, "updated_at": now()}])


def cmd_remove(args: argparse.Namespace) -> int:
    return send_commands([{"action": "remove", "product_id": args.product_id.strip(),
                           "updated_at": now()}])


def cmd_load(args: argparse.Namespace) -> int:
    with open(args.file, encoding="utf-8") as f:
        items = json.load(f)
    commands = [{"action": "add", "product_id": str(i["product_id"]).strip(),
                 "reason": i.get("reason", ""), "updated_at": now()} for i in items]
    log.info("Загрузка %d позиций из %s", len(commands), args.file)
    return send_commands(commands)


def read_state() -> dict[str, dict]:
    """Читает топик с начала до конца и восстанавливает текущее состояние списка."""
    consumer = make_consumer(IDENTITY, group_id="moderator-cli", **{"enable.partition.eof": True})
    try:
        metadata = consumer.list_topics(TOPIC_BANNED_PRODUCTS, timeout=10)
        topic_meta = metadata.topics[TOPIC_BANNED_PRODUCTS]
        if topic_meta.error is not None:
            raise KafkaException(topic_meta.error)
        partitions = [TopicPartition(TOPIC_BANNED_PRODUCTS, p, OFFSET_BEGINNING)
                      for p in topic_meta.partitions]
        consumer.assign(partitions)

        state: dict[str, dict] = {}
        finished: set[int] = set()
        idle = 0
        while len(finished) < len(partitions) and idle < 20:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            idle = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(msg.partition())
                    continue
                raise KafkaException(msg.error())
            if msg.value() is None:
                continue
            try:
                cmd = json.loads(msg.value())
                pid = str(cmd["product_id"])
            except (ValueError, KeyError, TypeError):
                log.warning("Пропущена некорректная запись offset=%s", msg.offset())
                continue
            if cmd.get("action") == "add":
                state[pid] = cmd
            else:
                state.pop(pid, None)
        return state
    finally:
        consumer.close()


def cmd_list(args: argparse.Namespace) -> int:
    state = read_state()
    print(f"\nЗапрещённых товаров: {len(state)}")
    for pid, cmd in sorted(state.items()):
        print(f"  {pid:<10} {cmd.get('reason') or '-':<45} с {cmd.get('updated_at')}")
    return 0


def main() -> int:
    setup_logging()
    parser = argparse.ArgumentParser(prog="banned-products",
                                     description="Управление списком запрещённых товаров")
    sub = parser.add_subparsers(dest="command", required=True)

    add = sub.add_parser("add", help="запретить товар")
    add.add_argument("--product-id", required=True)
    add.add_argument("--reason", default="")
    add.set_defaults(func=cmd_add)

    remove = sub.add_parser("remove", help="снять запрет")
    remove.add_argument("--product-id", required=True)
    remove.set_defaults(func=cmd_remove)

    lst = sub.add_parser("list", help="показать текущий список")
    lst.set_defaults(func=cmd_list)

    load = sub.add_parser("load", help="загрузить список из JSON-файла")
    load.add_argument("--file", default="data/banned-products.json")
    load.set_defaults(func=cmd_load)

    args = parser.parse_args()
    try:
        return args.func(args)
    except (OSError, ValueError, KafkaException) as exc:
        log.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
