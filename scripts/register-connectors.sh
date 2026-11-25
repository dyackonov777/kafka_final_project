
#!/usr/bin/env bash

# Регистрация и обновление коннекторов Kafka Connect.
# Ожидание REST API и состояния RUNNING.

set -o pipefail

source "$(dirname "$0")/common.sh"

# curl.exe для Windows Git Bash, обычный curl для Linux.
if [[ "$(uname -s)" == MINGW* || \
      "$(uname -s)" == MSYS* || \
      "$(uname -s)" == CYGWIN* ]]; then
  HTTP_CURL="curl.exe"
else
  HTTP_CURL="curl"
fi

# --------------------------------------------------
# 1. Ожидание Kafka Connect REST API
# --------------------------------------------------

echo
echo "  ожидание Kafka Connect REST API"

waited=0
CONNECT_TIMEOUT=240

while [ "$waited" -lt "$CONNECT_TIMEOUT" ]; do

  if "$HTTP_CURL" -fsS \
      --connect-timeout 3 \
      --max-time 5 \
      "$CONNECT_URL/" >/dev/null 2>&1; then

    echo "  [ok] Kafka Connect REST API доступен (${waited} с)"
    break
  fi

  sleep 5
  waited=$((waited + 5))
done

if [ "$waited" -ge "$CONNECT_TIMEOUT" ]; then
  echo "  [!] Kafka Connect REST API недоступен"
  echo "  URL: $CONNECT_URL/"

  docker compose logs --tail=100 kafka-connect
  exit 1
fi

# --------------------------------------------------
# 2. Регистрация коннекторов
# --------------------------------------------------

echo
echo "  регистрация коннекторов"

for f in config/connect/*.json; do

  [ -f "$f" ] || continue

  name=$(basename "$f" .json)

  echo "  регистрируем: $name"

  if ! resp=$("$HTTP_CURL" -fsS \
      --connect-timeout 5 \
      --max-time 30 \
      -X PUT "$CONNECT_URL/connectors/$name/config" \
      -H 'Content-Type: application/json' \
      --data-binary "@$f" 2>&1); then

    echo "  [!] Коннектор $name не зарегистрирован:"
    echo "$resp" >&2
    exit 1
  fi

  echo "  [+] Коннектор $name зарегистрирован"

done

# --------------------------------------------------
# 3. Ожидание запуска коннекторов
# --------------------------------------------------

echo
echo "  ожидание запуска коннекторов"

for f in config/connect/*.json; do

  [ -f "$f" ] || continue

  name=$(basename "$f" .json)

  waited=0
  timeout=240
  status=""

  echo
  echo "  ждём: $name"

  while [ "$waited" -lt "$timeout" ]; do

    status=$("$HTTP_CURL" -fsS \
      --connect-timeout 3 \
      --max-time 5 \
      "$CONNECT_URL/connectors/$name/status" \
      2>/dev/null || true)

    # Проверяем состояние connector.
    if echo "$status" | grep -q '"state":"FAILED"'; then

      echo "  [!] $name находится в состоянии FAILED"
      echo "$status"
      docker compose logs --tail=100 kafka-connect
      exit 1
    fi

    # Проверяем состояние всех задач.
    connector_running=false
    tasks_running=false

    if echo "$status" | grep -q '"connector":{"state":"RUNNING"'; then
      connector_running=true
    fi

    if echo "$status" | grep -q '"tasks":\[{.*"state":"RUNNING"'; then
      tasks_running=true
    fi

    if [ "$connector_running" = true ] && \
       [ "$tasks_running" = true ]; then

      echo "  [ok] $name: RUNNING (${waited} с)"
      break
    fi

    sleep 5
    waited=$((waited + 5))

  done

  if [ "$waited" -ge "$timeout" ]; then

    echo "  [!] Таймаут ожидания коннектора $name"
    echo
    echo "  Последний статус:"
    echo "$status"

    echo
    echo "  Логи Kafka Connect:"
    docker compose logs --tail=100 kafka-connect

    exit 1
  fi

done

# --------------------------------------------------
# 4. Результат
# --------------------------------------------------

echo
echo "  [+] Все коннекторы успешно запущены"
echo
