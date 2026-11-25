
#!/usr/bin/env bash
# Общие настройки для скриптов.
# Подключение: source "$(dirname "$0")/common.sh"
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Отключаем преобразование Windows-путей в Git Bash.
export MSYS_NO_PATHCONV=1

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source <(tr -d '\r' < .env)
  set +a
fi

ES_URL="${ES_URL:-http://localhost:9200}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

# Python для Linux/macOS или Windows.
if python3 -c "" >/dev/null 2>&1; then
  PY=python3
else
  PY=python
fi

dc() {
  docker compose "$@"
}

PRIMARY_BS="kafka-1:19092,kafka-2:19092,kafka-3:19092"
ANALYTICS_BS="kafka-b1:19092,kafka-b2:19092,kafka-b3:19092"

# Kafka CLI: основной кластер.
kafka_admin() {
  local tool=$1
  shift

  dc exec -T kafka-tools "$tool" \
    --bootstrap-server "$PRIMARY_BS" \
    --command-config /etc/kafka/clients/admin.properties "$@"
}

# Kafka CLI: аналитический кластер.
kafka_admin_analytics() {
  local tool=$1
  shift

  dc exec -T kafka-tools "$tool" \
    --bootstrap-server "$ANALYTICS_BS" \
    --command-config /etc/kafka/clients/admin.properties "$@"
}

# Офсеты основного кластера.
topic_end_offset() {
  kafka_admin kafka-get-offsets --topic "$1" 2>/dev/null |
    awk -F: '{s+=$3} END {print s+0}'
}

# Офсеты аналитического кластера.
analytics_topic_end_offset() {
  kafka_admin_analytics kafka-get-offsets --topic "$1" 2>/dev/null |
    awk -F: '{s+=$3} END {print s+0}'
}

# Elasticsearch.
# ВАЖНО:
# Git Bash в Windows может выдавать curl error 23,
# когда ответ перенаправляется в /dev/null.
# Поэтому используем --output NUL в Windows Git Bash
# и /dev/null в Linux/macOS.


# Elasticsearch: запросы через Windows curl.exe или Linux curl.
es_curl() {
  local args=()
  local skip_next=false
  local output_requested=false

  for arg in "$@"; do
    if [ "$skip_next" = true ]; then
      skip_next=false
      continue
    fi

    case "$arg" in
      -o|--output)
        skip_next=true
        output_requested=true
        ;;
      *)
        args+=("$arg")
        ;;
    esac
  done

  local curl_cmd=curl
  local null_device=/dev/null

  if [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; then
    curl_cmd=curl.exe
    null_device=NUL
  fi

  if [ "$output_requested" = true ]; then
    "$curl_cmd" -fsS \
      -u "elastic:${ELASTIC_PASSWORD}" \
      -o "$null_device" \
      "${args[@]}"
  else
    "$curl_cmd" -fsS \
      -u "elastic:${ELASTIC_PASSWORD}" \
      "${args[@]}"
  fi
}


# Количество документов в индексе.
es_count() {
  es_curl "$ES_URL/$1/_count" 2>/dev/null |
    sed -n 's/.*"count"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' |
    head -n 1 |
    grep . || echo 0
}

# Ожидание условия.
# wait_until <описание> <таймаут> <команда...>
wait_until() {
  local what=$1
  local timeout=$2
  local waited=0
  shift 2

  printf '  ждём: %s' "$what"

  until "$@" >/dev/null 2>&1; do
    if [ "$waited" -ge "$timeout" ]; then
      echo " — не дождались за ${timeout} с"
      return 1
    fi

    printf '.'
    sleep 3
    waited=$((waited + 3))
  done

  echo " — готово (${waited} с)"
}

# Ожидание HTTP-эндпоинта.
wait_http() {
  wait_until "$2" "${3:-180}" curl -fsS -o /dev/null "$1"
}

# Проверки Elasticsearch.
es_count_at_least() {
  [ "$(es_count "$1")" -ge "$2" ]
}

es_doc_exists() {
  es_curl "$ES_URL/$1/_doc/$2"
}

es_doc_missing() {
  ! es_doc_exists "$1" "$2"
}

# Проверка синхронизации кластеров Kafka.
mirror_caught_up() {
  local t src dst

  for t in "$@"; do
    src=$(topic_end_offset "$t")
    dst=$(analytics_topic_end_offset "primary.$t")

    [ "$src" -gt 0 ] && [ "$dst" -ge "$src" ] || return 1
  done
}

step() {
  echo
  echo "==== $*"
}
