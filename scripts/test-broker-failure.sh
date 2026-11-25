#!/usr/bin/env bash
# Проверка отказоустойчивости и алертинга:
#   1) останавливаем брокер kafka-2;
#   2) запись с acks=all продолжается (RF=3, min.insync.replicas=2 -> достаточно двух реплик);
#   3) Prometheus фиксирует up=0, Alertmanager отправляет KafkaBrokerDown (webhook / Telegram);
#   4) возвращаем брокер, алерт переходит в resolved.
source "$(dirname "$0")/common.sh"

BROKER=${1:-kafka-2}

step "Останавливаем $BROKER"
dc stop "$BROKER"

step "Запись при одном недоступном брокере (acks=all, min.insync.replicas=2)"
bash cli.sh shop send

step "Ждём срабатывания алерта (scrape 15s + for 30s)..."
for _ in $(seq 1 18); do
  alerts=$(curl -fsS "http://localhost:9090/api/v1/alerts" 2>/dev/null || true)
  if echo "$alerts" | grep -q '"alertname":"KafkaBrokerDown".*"state":"firing"\|"state":"firing".*"alertname":"KafkaBrokerDown"'; then
    echo "  KafkaBrokerDown: firing"
    break
  fi
  sleep 5
done
sleep 15
echo
echo "Алерты в Alertmanager:"
curl -fsS "http://localhost:9099/api/v2/alerts" | $PY -c '
import json, sys
for a in json.load(sys.stdin):
    print("  -", a["labels"].get("alertname"), "|", a["labels"].get("broker", "-"), "|", a["status"]["state"], "|", a["annotations"].get("summary", ""))
' || true
echo
echo "Получатель webhook (alert-webhook):"
dc logs --since 2m alert-webhook | tail -10

step "Возвращаем $BROKER"
dc start "$BROKER"
echo "Через ~1 минуту алерт перейдёт в RESOLVED: docker compose logs -f alert-webhook"
