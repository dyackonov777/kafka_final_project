#!/usr/bin/env bash
# Остановка платформы.
#   bash stop.sh          # остановить и удалить контейнеры, данные (volumes) сохраняются
#   bash stop.sh --clean  # удалить также все данные: топики, индексы, Data Lake, метрики
source "$(dirname "$0")/scripts/common.sh"

if [ "${1:-}" = "--clean" ]; then
  dc down -v --remove-orphans
else
  dc down --remove-orphans
fi
