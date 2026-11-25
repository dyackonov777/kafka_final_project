# Аналитическая платформа маркетплейса «Покупай выгодно»

Итоговый проект курса по Apache Kafka. Платформа собирает данные о товарах магазинов, запросах
и заказах клиентов, фильтрует запрещённые товары в реальном времени, хранит данные для поиска,
дублирует их во второй кластер Kafka, непрерывно выгружает в Data Lake (HDFS), считает аналитику
и рекомендации на Spark и мониторит всю цепочку через Prometheus, Grafana и Alertmanager.

Весь стенд работает локально в Docker Compose и поднимается одной командой `bash start.sh`.

## Содержание

1. [Архитектура](#архитектура)
2. [Соответствие критериям](#соответствие-критериям)
3. [Технологии](#технологии)
4. [Запуск](#запуск)
5. [Команды CLI](#команды-cli)
6. [Реализация по шагам задания](#реализация-по-шагам-задания)
7. [Безопасность](#безопасность)
8. [Проверка работы](#проверка-работы)
9. [Результаты запуска](#результаты-запуска)
10. [Структура проекта](#структура-проекта)
11. [Решение проблем](#решение-проблем)

---

## Архитектура

```text
1. ИСТОЧНИКИ ДАННЫХ → ОСНОВНОЙ КЛАСТЕР KAFKA

  ┌──────────────────────┐                 ┌──────────────────────────────────────────┐
  │ SHOP API             │     товары      │ KAFKA PRIMARY  kafka-1, kafka-2, kafka-3 │
  │ data/products.json   │────────────────►│ KRaft · mTLS + ACL · RF=3 · min.insync=2 │
  └──────────────────────┘                 │                                          │
  ┌──────────────────────┐     запросы     │   shop-products                          │
  │ CLIENT API           │────────────────►│   client-requests                        │
  │ search / recommend   │     заказы      │   orders                                 │
  │ buy / orders/history │────────────────►│   products-filtered                      │
  └──────────────────────┘                 │   shop-products-dlq                      │
  ┌──────────────────────┐  add / remove   │   banned-products   (compact)            │
  │ CLI модератора       │────────────────►│   recommendations   (compact)            │
  │ список запрещённых   │                 │                                          │
  └──────────────────────┘                 └──────────────────────────────────────────┘

2. ПОТОКОВАЯ ОБРАБОТКА (Faust): фильтр запрещённых товаров и резервы по заказам

  shop-products ─────┐                          ┌──► products-filtered   разрешённые товары;
                     │    ┌──────────────────┐  │                        tombstone при запрете;
  banned-products ───┼───►│ FAUST            │──┤                        остаток с учётом резерва
                     │    │ faust-moderation │  │
  orders ────────────┘    └──────────────────┘  └──► shop-products-dlq   нечитаемые сообщения

3. ХРАНЕНИЕ И ПОИСК (Kafka Connect)

  products-filtered ──┐                         ┌──► Elasticsearch: индексы products-filtered,
  client-requests ────┤    ┌───────────────┐    │    client-requests, orders, recommendations
  orders ─────────────┼───►│ KAFKA CONNECT │────┤      ▲
  recommendations ────┘    └───────────────┘    │      └── CLIENT API: поиск, заказы, рекомендации
                                                │
                                                └──► файл /data/sink/products-filtered.jsonl
                                                     (только products-filtered, без tombstone)

4. ДУБЛИРОВАНИЕ ВО ВТОРОЙ КЛАСТЕР И АНАЛИТИКА

  KAFKA PRIMARY             ┌───────────────┐        KAFKA ANALYTICS   kafka-b1, kafka-b2, kafka-b3
                            │               │        KRaft · mTLS + ACL · RF=3
  shop-products ───────────►│               │──────► primary.shop-products
  products-filtered ───────►│ MIRRORMAKER 2 │──────► primary.products-filtered ───┐
  client-requests ─────────►│               │──────► primary.client-requests ─────┤
  orders ──────────────────►│               │──────► primary.orders ──────────────┤
                            └───────────────┘                                     │
                                                                                  ▼
                                                                  ┌───────────────────────────┐
                                                                  │ spark-ingest              │
                                                                  │ Streaming, микробатч 15 с │
                                                                  └─────────────┬─────────────┘
                                                                                │ Parquet
                                                                                ▼
  ┌──────────────────────────┐        ┌───────────────────────┐   ┌───────────────────────────┐
  │ KAFKA PRIMARY            │        │ Spark batch           │   │ HDFS (Data Lake)          │
  │ recommendations, compact │◄───────│ витрины, рекомендации │◄──│ /datalake/raw             │
  │ ключ = user_id           │  mTLS  │ run-analytics.sh      │   │ /datalake/marts           │
  └──────────────────────────┘        └───────────────────────┘   └───────────────────────────┘
       │
       └──► Kafka Connect ──► Elasticsearch ──► CLIENT API recommend

5. МОНИТОРИНГ

  брокеры обоих кластеров ── JMX Exporter :7071 ──┐
  Kafka Connect ──────────── JMX Exporter :9404 ──┤
  lag групп, офсеты, MM2 ─── kafka-exporter ──────┼──► Prometheus ──► Grafana (дашборд)
  Faust ──────────────────── /metrics :6066 ──────┘        │
                                                           └──► Alertmanager ──► webhook / Telegram
```

**Поток данных:**

1. **SHOP API** читает товары из `data/products.json` и пишет их в `shop-products`.
2. **Faust** сверяет каждый товар со списком запрещённых (таблица из топика `banned-products`):
   - разрешённые товары уходят в `products-filtered`, нечитаемые сообщения — в `shop-products-dlq`;
   - если товар запрещают, когда он уже в хранилище, Faust отправляет в `products-filtered` *tombstone*, и товар удаляется из Elasticsearch и из каталога аналитики;
   - по заказам из `orders` Faust ведёт резерв на складе и публикует товар с обновлённым остатком.
3. **Kafka Connect** пишет отфильтрованные товары в Elasticsearch и в файл (отладочное хранилище). В Elasticsearch также попадают запросы клиентов, заказы и рекомендации.
4. **CLIENT API** ищет товары в Elasticsearch, оформляет заказы (топик `orders`) и публикует каждый запрос в `client-requests`.
5. **MirrorMaker 2** копирует топики во второй кластер `analytics` (3 брокера, TLS, ACL).
6. **spark-ingest** непрерывно переносит данные из второго кластера в HDFS. **Spark batch** считает витрины (каталог, спрос, продажи) и рекомендации и пишет их в топик `recommendations` основного кластера. Оттуда Connect переносит рекомендации в Elasticsearch, и CLIENT API выдаёт их по `recommend`.

---

## Соответствие критериям

| Критерий | Где реализовано | Как проверить |
|---|---|---|
| Kafka передаёт данные между сервисами | SHOP API → Faust → Connect → ES/файл; CLIENT API → Kafka → MM2 → HDFS → Spark → Kafka → ES | `bash scripts/demo.sh`, `bash scripts/check-pipeline.sh` |
| TLS включён | оба кластера: все listener'ы, включая контроллеры KRaft, работают по SSL, `ssl.client.auth=required` | `bash scripts/check-security.sh` (проверки 1, 2, 9) |
| ACL ограничивают доступ к топикам | `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`, отдельный сертификат и ACL для каждого сервиса, без wildcard-прав ([матрица](#acl)) | `check-security.sh` (проверки 3–7a, 10) |
| Репликация, минимум синхронных реплик | оба кластера: RF=3, `min.insync.replicas=2`, `acks=all`, `unclean.leader.election.enable=false` | `check-security.sh` (8, 11), `bash scripts/test-broker-failure.sh` |
| Дублирование во второй кластер | MirrorMaker 2: `primary → analytics` (3 брокера, TLS, ACL) | `check-pipeline.sh`, Kafka UI → кластер `analytics`, панель «Отставание MirrorMaker 2» |
| Фильтрация запрещённых товаров | Faust (`app/marketplace/moderation.py`), список управляется через CLI; запрет действует и на уже сохранённые товары | `demo.sh` шаги 2, 3, 9, 10 |
| Отфильтрованные данные в хранилище | Elasticsearch Sink + FileStream Sink читают только `products-filtered` | `check-pipeline.sh`: ID товаров в ES, файл |
| Аналитическая обработка | Spark: HDFS → витрины `/datalake/marts/*` (каталог, спрос, продажи, топ товаров) | `bash scripts/run-analytics.sh` |
| Рекомендации в отдельном топике | топик `recommendations` (compact) | `bash cli.sh client recommend --user-id user_001` |
| Мониторинг: Prometheus + JMX Exporter | JMX Exporter на брокерах обоих кластеров и в Connect, kafka-exporter (lag), `/metrics` Faust | http://localhost:9090/targets |
| Мониторинг: дашборд Grafana | `monitoring/grafana/dashboards/kafka-cluster.json` (34 панели) | http://localhost:3000 → Kafka |
| Мониторинг: Alertmanager при сбое | 13 правил, в том числе `KafkaBrokerDown`; webhook-получатель и Telegram (опционально) | `bash scripts/test-broker-failure.sh` |
| Документация | этот README | — |

Помимо обязательной части сделано:
- **Шаг 5, расширенный вариант:** Elasticsearch с индексами товаров и **заказов** (а также запросов клиентов и рекомендаций), явными маппингами и аутентификацией. Базовый вариант (файл) тоже сделан.
- **Заказы и склад:** `client buy` оформляет заказ. Faust учитывает резерв, и остаток товара в хранилище уменьшается. Покупки влияют на рекомендации.
- **Мониторинг всей цепочки:** lag потребителей, отставание MirrorMaker 2, статусы коннекторов, метрики Faust, алерты на каждый компонент.
- **Telegram-уведомления** (включаются через `.env`), **Kafka UI** для обоих кластеров с формой входа.

---

## Технологии

| Компонент | Версия | Назначение |
|---|---|---|
| Apache Kafka (Confluent Platform) | 7.6.1 (Kafka 3.6), KRaft | основной и аналитический кластеры, по 3 брокера |
| MirrorMaker 2 | из Kafka 3.6 | дублирование данных во второй кластер |
| Faust (faust-streaming) + aiokafka | 0.15.3 + 0.14.0, Python 3.12 | потоковая фильтрация, учёт резервов |
| confluent-kafka (librdkafka) | 2.5.3 | SHOP API, CLIENT API, CLI |
| Kafka Connect | 7.6.1 | Elasticsearch Sink 15.0.0 (Confluent Hub), FileStream Sink (из образа, `/usr/share/filestream-connectors`) |
| Elasticsearch | 7.17.25 (X-Pack security) | хранение и поиск: товары, заказы, запросы, рекомендации |
| Apache Hadoop HDFS | 3.3.6 | Data Lake |
| Apache Spark | 3.5.6 (standalone), PySpark | непрерывная загрузка в HDFS, аналитика и рекомендации |
| Prometheus JMX Exporter | 0.20.0 | метрики брокеров и Kafka Connect |
| kafka-exporter (danielqsj) | 1.7.0 | lag групп потребителей, офсеты топиков |
| Prometheus / Alertmanager / Grafana | 2.51.2 / 0.27.0 / 10.4.2 | метрики, алерты, дашборд |
| Kafka UI (provectus) | 0.7.2 | просмотр обоих кластеров |
| OpenSSL | ≥ 3.2 | собственный CA, сертификаты брокеров и клиентов |

---

## Запуск

### Требования

- Docker с Compose v2. Docker должно быть доступно **не меньше 16 ГБ RAM**: в стенде 6 брокеров, Elasticsearch, HDFS и Spark.
- bash: Linux, macOS или **Git Bash** в Windows. Проект проверен в Windows (Git Bash + Docker Desktop). В Git Bash скрипты сами переключаются на `curl.exe` для HTTP-запросов к Elasticsearch и Kafka Connect: у встроенного `curl` Git Bash при выводе в `/dev/null` бывает ошибка 23.
- OpenSSL 3.2 или новее (`openssl version`; в свежем Git for Windows он уже есть).
- `curl`. При первом запуске нужен интернет: скачиваются образы, JMX Exporter и Elasticsearch-коннектор, а Spark загружает `spark-sql-kafka` из Maven Central.

### Одной командой

```bash
bash start.sh
```

Скрипт выполняет шаги по порядку:

1. Создаёт `.env` из `.env.example` (**пароли необходимо поменять**) и генерирует сертификаты в `certs/`. `.env` и `certs/` есть в `.gitignore`.
2. Собирает образы: брокер с JMX Exporter, Kafka Connect с коннекторами и JMX Exporter, Python-приложение. При сборке проверяется, что jar JMX Exporter скачался и не повреждён (`jar tf`), иначе сборка падает сразу, а не при старте брокера.
3. Поднимает оба кластера Kafka, создаёт топики и ACL основного кластера (`scripts/kafka/init-topics-acl.sh`) и ACL аналитического (`scripts/kafka/init-analytics-acl.sh`).
4. Поднимает остальные сервисы. **У каждого сервиса есть healthcheck**, и `docker compose up --wait` ждёт, пока сервис действительно готов. Например, MirrorMaker 2 считается готовым, когда создал копии топиков, а datanode — когда namenode видит его живым.
5. Создаёт в Elasticsearch роли, пользователей и индексы с маппингами, затем регистрирует коннекторы (`scripts/register-connectors.sh`):
   - ждёт ответа REST API Kafka Connect (до 240 с);
   - регистрирует каждый коннектор через идемпотентный `PUT /connectors/<name>/config`, при ошибке HTTP останавливается (`curl -f`);
   - ждёт, пока коннектор **и его задачи** перейдут в `RUNNING`. Если коннектор в `FAILED` или не запустился за 240 с, печатает статус и последние 100 строк лога `kafka-connect` и завершается с ошибкой.
6. Загружает начальный список запрещённых товаров.

Первый запуск занимает 5–10 минут. Повторно запускать безопасно: все шаги идемпотентны.

Затем необходимо запустить сквозной сценарий:

```bash
bash scripts/demo.sh
```

Остановка: `bash stop.sh` (данные сохраняются) или `bash stop.sh --clean` (удаляет всё).

### Веб-интерфейсы

Все порты опубликованы только на `127.0.0.1`. Адрес задаётся переменной `BIND_ADDRESS` в `.env`.

| Сервис | Адрес | Вход |
|---|---|---|
| Kafka UI (оба кластера) | http://localhost:8080 | `KAFKA_UI_USER` / `KAFKA_UI_PASSWORD` |
| Grafana | http://localhost:3000 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| Elasticsearch | http://localhost:9200 | `elastic` / `ELASTIC_PASSWORD` |
| Prometheus | http://localhost:9090 (`/targets`, `/alerts`) | — |
| Alertmanager | http://localhost:9099 | — |
| Kafka Connect REST | http://localhost:8083/connectors | — |
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8090 | — |

Порты Kafka на хосте (только TLS с клиентским сертификатом): primary — `localhost:9093–9095`, analytics — `localhost:9096–9098`.

---

## Команды CLI

Команды выполняются через обёртку `cli.sh`. У каждой группы команд **свой контейнер и свой сертификат**: `shop-api-cli` (CN=shop-api), `client-api-cli` (CN=client-api), `moderator-cli` (CN=moderator).

```bash
# SHOP API: отправить товары из файла (файл хранится в Git: data/products.json)
bash cli.sh shop send
bash cli.sh shop send --file data/products.json --delay 0.5
bash cli.sh shop send --inject-bad 2            # плюс 2 битых сообщения: проверка DLQ

# CLIENT API
bash cli.sh client search "умные часы" --user-id user_001
bash cli.sh client recommend --user-id user_001
bash cli.sh client buy --user-id user_001 --product-id 12345 --quantity 1
bash cli.sh client orders --user-id user_001    # заказы пользователя из Elasticsearch
bash cli.sh client history --user-id user_001   # запросы пользователя из Elasticsearch

# Список запрещённых товаров
bash cli.sh banned list
bash cli.sh banned add --product-id 12350 --reason "Нет сертификата"
bash cli.sh banned remove --product-id 12350
bash cli.sh banned load --file data/banned-products.json

# Аналитика (загрузка в HDFS идёт постоянно в сервисе spark-ingest)
bash scripts/run-analytics.sh
```

---

## Реализация по шагам задания

### Шаг 1. Источники данных

**SHOP API** (`app/marketplace/shop_api.py`):
- Читает JSON-массив товаров из `data/products.json` (20 товаров, из них 3 запрещённых) в формате из задания.
- Проверяет обязательные поля. Некорректный товар пропускается с предупреждением и не роняет отправку.
- Пишет в `shop-products` с ключом `product_id`. Настройки продюсера: `acks=all`, `enable.idempotence=true`, партиционер `murmur2_random`. Это тот же алгоритм, что у Java-клиентов и Faust, поэтому товар, команда его запрета и резерв попадают в партицию с одним номером.
- В итоге выводит число отправленных, подтверждённых, недоставленных и пропущенных товаров. Если хоть одно сообщение не доставлено, команда завершается с кодом 1.

**CLIENT API** (`app/marketplace/client_api.py`). В Elasticsearch ходит пользователем `client_api` с правами только на чтение.

| Команда | Что делает |
|---|---|
| `search <название>` | поиск по индексу `products-filtered`: точная фраза с повышенным весом и все слова с допуском опечаток, русский анализатор |
| `recommend --user-id` | рекомендации из индекса `recommendations`. Товары, которых уже нет в хранилище (например, запрещённые после расчёта), скрываются |
| `buy --user-id --product-id [--quantity]` | заказ. Товар берётся из хранилища, поэтому запрещённый купить нельзя. Проверяется остаток. Заказ пишется в топик `orders` (ключ `order_id`) |
| `orders --user-id` | заказы пользователя из индекса `orders` |
| `history --user-id` | история запросов из индекса `client-requests` |

Каждый запрос публикуется событием в `client-requests` (`event_id`, `event_type`, `user_id`, `query`, `result_product_ids`, `created_at`). Дальше он попадает в Elasticsearch через Connect и в аналитику через MM2 и HDFS. Так выполняется требование, чтобы запросы попадали **и в Kafka, и в систему хранения**.

### Шаг 2. Kafka: TLS, топики, ACL, отказоустойчивость

**Кластеры** (`docker-compose.yml`). Оба кластера по 3 узла KRaft (broker+controller) с одинаковыми настройками безопасности и надёжности (общий YAML-якорь `x-kafka-common-env`).

| Кластер | Брокеры | Порты на хосте | Назначение |
|---|---|---|---|
| primary | `kafka-1..3` | 9093–9095 | рабочий кластер платформы |
| analytics | `kafka-b1..3` | 9096–9098 | копия данных для аналитики (MirrorMaker 2) |

| Listener | Порт | Назначение |
|---|---|---|
| `INTERNAL_SSL` | 19092 | межброкерный трафик и клиенты в docker-сети |
| `EXTERNAL_SSL` | 9092 → порт хоста | клиенты с хоста |
| `CONTROLLER` (SSL) | 29093 | кворум контроллеров KRaft |

**TLS** (`scripts/generate-certs.sh`):
- Собственный CA. У каждого брокера свой keystore и truststore, SAN `DNS:<broker>, DNS:localhost`.
- У **каждого сервиса свой клиентский сертификат**. Principal в ACL равен Subject сертификата, `User:CN=<имя>`.
- `ssl.client.auth=required` (mTLS) и `ssl.endpoint.identification.algorithm=https`: клиент проверяет, что имя брокера совпадает с SAN.

**Топики основного кластера** (`scripts/kafka/init-topics-acl.sh`). У всех 3 партиции, `replication.factor=3`, `min.insync.replicas=2`:

| Топик | Политика | Кто пишет | Кто читает |
|---|---|---|---|
| `shop-products` | delete, 7 дней | SHOP API | Faust, MM2 |
| `banned-products` | **compact** (последнее состояние по `product_id`) | CLI модератора | Faust, CLI |
| `products-filtered` | delete, 7 дней | Faust | Connect, MM2 |
| `shop-products-dlq` | delete, 14 дней | Faust | администратор |
| `client-requests` | delete, 14 дней | CLIENT API | Connect, MM2 |
| `orders` | delete, 30 дней | CLIENT API | Faust (резервы), Connect, MM2 |
| `recommendations` | **compact** (последние рекомендации по `user_id`) | Spark | Connect |

Служебные топики Faust (changelog таблиц, топик резервов) создаёт сам Faust с префиксом `faust-moderation-` и RF=3.

<a id="acl"></a>**ACL**. Используются `StandardAuthorizer` и `allow.everyone.if.no.acl.found=false`: всё, что не разрешено явно, запрещено. **Wildcard-прав (`*`) нет**: даже мониторинг и Kafka UI видят только перечисленные бизнес-топики и группы.

Основной кластер (super users: брокеры и `admin`):

| Principal | Права |
|---|---|
| `CN=shop-api` | Write `shop-products` |
| `CN=moderation` (Faust) | Read `shop-products`, `banned-products`, `orders`; Write `products-filtered`, `shop-products-dlq`; All на префикс `faust-moderation-`; группа `faust-moderation` |
| `CN=moderator` (CLI) | Read/Write `banned-products`; группы `moderator-*` |
| `CN=client-api` | Write `client-requests`, `orders` |
| `CN=connect` | Read `products-filtered`, `client-requests`, `orders`, `recommendations`; All на префикс `connect-`; группы `connect-*` |
| `CN=mirror-maker` | **только чтение**: Read, Describe, DescribeConfigs топиков `shop-products`, `products-filtered`, `client-requests`, `orders` (DescribeConfigs нужен MM2, чтобы прочитать конфигурацию исходного топика) |
| `CN=analytics` (Spark) | Write `recommendations` |
| `CN=kafka-ui` | Read/Describe перечисленных бизнес-топиков, Describe групп платформы |
| `CN=kafka-exporter` | Describe перечисленных бизнес-топиков и групп платформы |

Аналитический кластер (`scripts/kafka/init-analytics-acl.sh`; super users: брокеры, `admin`, `mirror-maker`):

| Principal | Права |
|---|---|
| `CN=mirror-maker` | super user: создаёт топики-копии и служебные топики MM2. Дополнительно права выданы явно: Create на кластер; Read, Write, Describe, DescribeConfigs, Alter на префикс `primary.`. Так права MM2 видны в `kafka-acls --list` и сохранятся, если убрать его из super users |
| `CN=analytics` (Spark) | **только** Read/Describe топиков `primary.*`; группы `spark-kafka-source*` |
| `CN=kafka-exporter` | Describe топиков `primary.*` (офсеты копий для расчёта отставания MM2) |
| `CN=kafka-ui` | Describe/DescribeConfigs кластера; Read/Describe/DescribeConfigs топиков `primary.*`; группы `kafka-ui*` |

Каждый топик открыт на запись **только своему** продюсеру. Это проверяется автоматически в `scripts/check-security.sh`.

**Отказоустойчивость:**
- RF=3, `min.insync.replicas=2`, продюсеры с `acks=all`: подтверждённая запись есть минимум на двух брокерах. Каждый кластер переживает отказ одного брокера без остановки записи (`scripts/test-broker-failure.sh`).
- `unclean.leader.election.enable=false`, `auto.create.topics.enable=false`.

**Дублирование во второй кластер** (`config/mirror-maker/mm2.properties.tmpl`):
- MirrorMaker 2 копирует `shop-products`, `products-filtered`, `client-requests`, `orders` в кластер `analytics` под именами `primary.<топик>`, с RF=3 и `acks=all`.
- К обоим кластерам подключается по mTLS как `CN=mirror-maker`. В primary у него права только на чтение, в analytics он владелец (super user).
- Работает в выделенном режиме (`connect-mirror-maker`). Пароль keystore подставляется в конфиг при старте контейнера. Логирование настроено через `KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/etc/kafka/connect-log4j.properties`.
- `offset-syncs.topic.location=target` и `analytics->primary.emit.heartbeats.enabled=false`: в основной кластер MM2 ничего не пишет.
- Отставание MM2 видно на дашборде, при росте срабатывает алерт `MirrorMakerLagHigh`.

### Шаг 3. Аналитическая система (базовый вариант: HDFS + Spark)

1. **Непрерывная загрузка в Data Lake** (сервис `spark-ingest`, `analytics/ingest_to_hdfs.py`). Spark Structured Streaming читает `primary.products-filtered`, `primary.client-requests`, `primary.orders` из **второго кластера** (mTLS, `CN=analytics`, только чтение) и раз в 15 секунд пишет Parquet в `hdfs://namenode:8020/datalake/raw/topic=…/ingest_date=…`. Офсеты хранятся в checkpoint в HDFS, поэтому после перезапуска загрузка продолжается с места остановки. Tombstone-сообщения сохраняются (`value = null`).
2. **Аналитика** (`analytics/recommendations.py`, запуск `bash scripts/run-analytics.sh`):
   - сначала **ждёт, пока Data Lake догонит Kafka**: сравнивает последний офсет каждой партиции в analytics-кластере с последним офсетом в HDFS, без фиксированных пауз;
   - каталог — последнее сообщение по каждому `product_id`; если последнее сообщение — tombstone, товар запрещён и в каталог не входит;
   - заказы дедуплицируются по `order_id`, поэтому повторная доставка не искажает продажи;
   - витрины в `/datalake/marts/`: `catalog_by_category`, `top_queries`, `category_demand`, `sales_by_category`, `top_products_sales`, `recommendations`.
3. **Рекомендации.** Строить модель машинного обучения задание не требует, поэтому используется простая эвристика:
   - взаимодействия имеют вес: товар в поисковой выдаче — 1, покупка — 5;
   - интерес к категории — сумма весов взаимодействий пользователя с её товарами;
   - популярность товара — сумма весов взаимодействий всех пользователей с ним;
   - `score = 10 × интерес + популярность`, в рекомендации идут топ-5 товаров в наличии, **кроме уже купленных**;
   - пользователи без истории получают самые популярные товары (стратегия `popular`).
4. **Запись в Kafka:** рекомендации пишутся в топик `recommendations` основного кластера с ключом `user_id`, по mTLS как `CN=analytics` (право только на запись в этот топик). Дальше Connect переносит их в Elasticsearch, и CLIENT API выдаёт их по `recommend`.

Ресурсы Spark: у worker 4 ядра, `spark-ingest` занимает 1 (`spark.cores.max=1`), пакетная аналитика — 2.

### Шаг 4. Потоковая обработка (Faust)

Файл `app/marketplace/moderation.py`, сервис `faust-moderation`.

| Агент | Вход | Что делает |
|---|---|---|
| `apply_ban_commands` | `banned-products` | ведёт таблицу запрещённых товаров. При **запрете** отправляет tombstone в `products-filtered`: Elasticsearch Sink удаляет документ, Spark исключает товар из каталога. При **снятии запрета** снова публикует последнюю версию товара |
| `moderate_products` | `shop-products` | запрещённый товар → `[BLOCKED]`, дальше не передаётся; разрешённый → `products-filtered`, при этом резерв по товару обнуляется (см. модель остатка ниже); нечитаемый → `shop-products-dlq` с текстом ошибки |
| `split_orders` | `orders` | раскладывает позиции заказа по `product_id` во внутренний топик резервов; повторно доставленный заказ пропускается (таблица `orders-processed`) |
| `apply_reservations` | топик резервов | увеличивает резерв товара и публикует его с обновлённым остатком (`available` уменьшается, `reserved` растёт) |

- **Список запрещённых товаров** хранится в compacted-топике `banned-products`, управляется через CLI. Изменения применяются на лету.
- **Модель остатка.** Магазин присылает фактический остаток, в котором уже учтены продажи. Платформа резервирует товар только по заказам, оформленным **после** последней версии товара от магазина:
  - публикуется `available = available магазина − резерв` и `reserved = reserved магазина + резерв`;
  - новая версия товара от магазина обнуляет резерв, поэтому одни и те же заказы не вычитаются дважды.

  Пример: магазин прислал 10 → заказ на 3 → в хранилище 7. Магазин учёл продажу и прислал 7 → резерв сброшен, в хранилище 7. Новый заказ на 2 → 5. Повторно доставленный заказ ничего не меняет.

  Отмена заказа и снятие резерва в модель не входят: в эмуляции заказы только создаются.
- **Состояние** (таблицы запрещённых товаров, последних версий, резервов, обработанных заказов) восстанавливается из changelog-топиков с RF=3.
- **Обработка ошибок:** все агенты обёрнуты в `try/except`, и одно битое сообщение не останавливает поток. Это проверяется командой `shop send --inject-bad N`.
- **Партиционирование:** все топики товаров имеют ключ `product_id`, 3 партиции и одинаковый хеш (murmur2), поэтому выполняется co-partitioning. Заказы (ключ `order_id`) перекладываются в топик резервов по `product_id`. Обработчик можно масштабировать на несколько экземпляров.
- **Метрики** на `faust-moderation:6066/metrics`: решения фильтра (`marketplace_moderation_decisions_total{decision}`), резервы и стандартные метрики Faust.

### Шаг 5. Хранение и поиск данных (расширенный и базовый варианты)

Kafka Connect подключается к primary по mTLS как `CN=connect`. Коннекторы регистрируются автоматически (`scripts/register-connectors.sh`).

| Коннектор | Топики | Куда пишет |
|---|---|---|
| `elasticsearch-sink` | `products-filtered`, `client-requests`, `orders`, `recommendations` | индексы с теми же именами; `_id` документа = ключ сообщения; tombstone удаляет документ (`behavior.on.null.values=delete`) |
| `file-sink` | `products-filtered` | `/data/sink/products-filtered.jsonl` — отладочное хранилище; tombstone отбрасываются SMT `Filter` + `RecordIsTombstone` |

Плагины в образе `marketplace/kafka-connect` (`docker/connect/Dockerfile`): Elasticsearch Sink 15.0.0 ставится с Confluent Hub в `/usr/share/confluent-hub-components`. `FileStreamSinkConnector` начиная с Kafka 3.2 не лежит в classpath Connect, но в образе `cp-kafka-connect` он есть в `/usr/share/filestream-connectors`. Этот каталог добавлен в `CONNECT_PLUGIN_PATH`.

- **В хранилище попадают только отфильтрованные данные**: коннекторы читают `products-filtered`, а запрещённый после сохранения товар удаляется tombstone'ом.
- Индексы создаются заранее с явными маппингами (`config/elasticsearch/*.json`): **товары**, **заказы** (позиции `nested`), запросы клиентов, рекомендации. Русский анализатор для названий, `keyword` для ID и категорий, `scaled_float` для цен, `date` для дат.
- Ошибки не теряются: `errors.tolerance=all` вместе с DLQ-топиком `connect-dlq-elasticsearch` и заголовками с причиной ошибки.
- **Аутентификация Elasticsearch** (`scripts/init-elasticsearch.sh`): роли `marketplace_writer` (Connect) и `marketplace_reader` (CLIENT API) с правами только на индексы платформы. Пароль Connect передаётся через `EnvVarConfigProvider` (`${env:ES_CONNECT_PASSWORD}`) и не виден ни в Git, ни в REST API Connect.

Проверка через API:

```bash
source .env
curl -u elastic:$ELASTIC_PASSWORD 'localhost:9200/_cat/indices?v'
curl -u elastic:$ELASTIC_PASSWORD 'localhost:9200/products-filtered/_search?q=name:часы&pretty'
curl -u elastic:$ELASTIC_PASSWORD -H 'Content-Type: application/json' 'localhost:9200/orders/_search?pretty' \
  -d '{"query":{"nested":{"path":"items","query":{"term":{"items.category":"Спорт"}}}}}'
docker compose exec kafka-connect cat /data/sink/products-filtered.jsonl
```

### Шаг 6. Мониторинг

**Сбор метрик (Prometheus, `monitoring/prometheus/prometheus.yml`):**

| Job | Источник | Что даёт |
|---|---|---|
| `kafka-primary`, `kafka-analytics` | JMX Exporter (java-агент) в каждом брокере, `:7071` | состояние брокеров, контроллера, репликации, трафик, задержки, JVM |
| `kafka-exporter-primary` | kafka-exporter (mTLS) | lag групп `faust-moderation` и `connect-*`, офсеты топиков |
| `kafka-exporter-analytics` | kafka-exporter (mTLS) | офсеты копий → отставание MirrorMaker 2 |
| `kafka-connect` | JMX Exporter в Connect, `:9404` | статусы коннекторов и задач, ошибки, DLQ |
| `faust-moderation` | `/metrics` Faust | решения фильтра, резервы, события, ребалансы |

**Дашборд Grafana** «Kafka — платформа «Покупай выгодно»» (34 панели):
- брокеры онлайн в обоих кластерах, активный контроллер KRaft;
- under-replicated, under-min-ISR и offline-партиции, лидеры по брокерам;
- трафик по топикам, байты in/out, запросы и задержки p50/p99, ISR shrink/expand, heap JVM;
- **lag потребителей**, **отставание MirrorMaker 2** по топикам;
- коннекторы RUNNING и упавшие задачи;
- решения Faust в минуту, зарезервированные единицы.

**Алерты** (`monitoring/prometheus/alert-rules.yml`):

| Группа | Алерты |
|---|---|
| Доступность | **`KafkaBrokerDown`** (`up == 0` дольше 30 с), `KafkaBrokersMissing`, `KafkaAnalyticsBrokerDown` |
| Репликация | `KafkaUnderReplicatedPartitions`, `KafkaUnderMinIsrPartitions`, `KafkaOfflinePartitions`, `KafkaNoActiveController` |
| Потребители и интеграции | `KafkaConsumerLagHigh`, `MirrorMakerLagHigh`, `FaustModerationDown`, `KafkaConnectDown`, `KafkaConnectorNotRunning`, `KafkaConnectTaskFailed` |

**Alertmanager** отправляет алерты на webhook. Сервис `alert-webhook` печатает их в лог: `docker compose logs -f alert-webhook`. **Telegram** включается, если заполнить `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID` в `.env` и выполнить `docker compose up -d alertmanager`; токен в Git не попадает.

### Шаг 7. Документация

Этот файл.

---

## Безопасность

| Что | Как защищено |
|---|---|
| Kafka (оба кластера) | mTLS у всех клиентов и межброкерного трафика, ACL без wildcard, отдельный сертификат у каждого сервиса |
| Сертификаты в контейнерах | каждый контейнер монтирует только свой сертификат. CLI разделены на `shop-api-cli`, `client-api-cli`, `moderator-cli` |
| Elasticsearch | X-Pack security: пользователи `kafka_connect` (запись) и `client_api` (только чтение) с правами только на индексы платформы |
| Kafka UI, Grafana | форма входа, пароли в `.env`; Kafka UI в режиме только для чтения, регистрация в Grafana отключена |
| Остальные UI (Prometheus, Alertmanager, Connect REST, HDFS, Spark) | собственной аутентификации нет, поэтому порты опубликованы только на `127.0.0.1` |
| Секреты | все пароли в `.env` (в `.gitignore`). Пароль keystore подставляется в конфиг MM2 при старте контейнера, пароль ES для Connect передаётся через `${env:...}` |

Ограничения учебного стенда: HTTP к Elasticsearch идёт без TLS (только внутри docker-сети и на `127.0.0.1`), файлы сертификатов открыты на чтение для всех UID контейнеров (`chmod a+r`). В production нужны TLS для HTTP-API, секреты в Vault или Docker secrets и права `0400` на ключи.

---

## Проверка работы

| Скрипт | Что показывает |
|---|---|
| `bash scripts/demo.sh` | весь сценарий: товары (с битым сообщением) → решения фильтра → поиск → заказы и резерв → MM2 → Spark → рекомендации → запрет уже сохранённого товара (удаление из ES, скрытие в рекомендациях) → снятие запрета → итог. Вместо `sleep` используются ожидания по условию (`wait_until`) |
| `bash scripts/check-security.sh` | 12 проверок: без сертификата и по PLAINTEXT не подключиться (оба кластера); чужие клиенты не могут писать и читать чужие топики; Spark не может писать в копии; RF=3 и `min.insync.replicas=2` |
| `bash scripts/check-pipeline.sh` | сообщения в топиках обоих кластеров (primary и копия рядом), ID товаров в ES, список запрещённых, индексы, файл sink, коннекторы, lag Faust, DLQ, объём Data Lake |
| `bash scripts/test-broker-failure.sh` | останавливает `kafka-2`, запись продолжается, срабатывает `KafkaBrokerDown`, алерт виден в Alertmanager и в `alert-webhook`, брокер возвращается |
| `bash scripts/run-analytics.sh` | витрины и рекомендации (вывод Spark), содержимое `/datalake` |

Ожидаемые результаты `demo.sh` на данных из репозитория:
- в Elasticsearch 17 товаров: 20 минус 3 запрещённых (`90001`, `90002`, `90003`); в `shop-products-dlq` одно битое сообщение;
- `client search "электронная сигарета"` ничего не находит, а `client buy --product-id 90001` отвечает «товар не найден или недоступен для продажи»;
- 4 заказа в индексе `orders`; у товара `22001` после заказа 2 шт. `available` уменьшился на 2;
- в рекомендациях нет уже купленных товаров;
- после `banned add --product-id 12346` товар исчезает из поиска и из рекомендаций, а после `banned remove` возвращается;
- копии `primary.*` в кластере `analytics` совпадают с оригиналами по числу сообщений.

---

## Результаты запуска

Стенд запускался 24.11.2026 в Windows 11 (Git Bash, Docker Desktop). Ниже выдержки из реального вывода `bash start.sh` и `bash scripts/demo.sh`, служебные строки сборки и Spark убраны. Скриншоты и полные логи запуска (`Запуск.txt`, `demo.txt`) лежат в каталоге [`screenshots/`](screenshots/).

### `bash start.sh`

**Шаг 1–2. Сертификаты и сборка образов.** Для каждого брокера обоих кластеров и для каждого клиента выпущен свой сертификат. Собраны три образа: брокер с JMX Exporter, Kafka Connect, Python-приложение.

![Генерация сертификатов](<screenshots/Снимок экрана 2026-11-24 193457.png>)

![Сборка образов](<screenshots/Снимок экрана 2026-11-24 193545.png>)

**Шаг 3. Кластеры, топики, ACL.** Все 6 брокеров и `kafka-tools` прошли healthcheck. Топики созданы с 3 партициями, RF=3 и `min.insync.replicas=2`:

![Топики основного кластера](<screenshots/Снимок экрана 2026-11-24 194944.png>)

```text
Topic: shop-products      PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000,unclean.leader.election.enable=false
Topic: shop-products-dlq  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=1209600000,unclean.leader.election.enable=false
Topic: products-filtered  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000,unclean.leader.election.enable=false
Topic: banned-products    PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,cleanup.policy=compact,unclean.leader.election.enable=false
Topic: client-requests    PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=1209600000,unclean.leader.election.enable=false
Topic: orders             PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=2592000000,unclean.leader.election.enable=false
Topic: recommendations    PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,cleanup.policy=compact,unclean.leader.election.enable=false
```

Служебные топики Faust (`faust-moderation-*-changelog`, `faust-moderation-stock-reservations`) и Kafka Connect (`connect-configs`, `connect-offsets`, `connect-status`, `connect-dlq-elasticsearch`) тоже создаются с RF=3 и `min.insync.replicas=2`.

ACL основного кластера. Пример для топика `shop-products`: писать может только `shop-api`, читать — только Faust (`moderation`), MirrorMaker 2 и Kafka UI.

```text
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=shop-products, patternType=LITERAL)`:
        (principal=User:CN=shop-api, host=*, operation=WRITE, permissionType=ALLOW)
        (principal=User:CN=shop-api, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=moderation, host=*, operation=READ, permissionType=ALLOW)
        (principal=User:CN=moderation, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=moderation, host=*, operation=CREATE, permissionType=ALLOW)
        (principal=User:CN=mirror-maker, host=*, operation=READ, permissionType=ALLOW)
        (principal=User:CN=mirror-maker, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=mirror-maker, host=*, operation=DESCRIBE_CONFIGS, permissionType=ALLOW)
        (principal=User:CN=kafka-exporter, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=kafka-ui, host=*, operation=READ, permissionType=ALLOW)
        (principal=User:CN=kafka-ui, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=kafka-ui, host=*, operation=DESCRIBE_CONFIGS, permissionType=ALLOW)

Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=recommendations, patternType=LITERAL)`:
        (principal=User:CN=analytics, host=*, operation=WRITE, permissionType=ALLOW)
        (principal=User:CN=analytics, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=connect, host=*, operation=READ, permissionType=ALLOW)
        (principal=User:CN=connect, host=*, operation=DESCRIBE, permissionType=ALLOW)
        ...
```

![ACL основного кластера](<screenshots/Снимок экрана 2026-11-24 195000.png>)

ACL аналитического кластера:

```text
== Итог: ACL аналитического кластера
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=primary., patternType=PREFIXED)`:
        (principal=User:CN=mirror-maker, host=*, operation=READ|WRITE|DESCRIBE|DESCRIBE_CONFIGS|ALTER, permissionType=ALLOW)
        (principal=User:CN=analytics, host=*, operation=READ, permissionType=ALLOW)
        (principal=User:CN=analytics, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=kafka-exporter, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=kafka-ui, host=*, operation=READ|DESCRIBE|DESCRIBE_CONFIGS, permissionType=ALLOW)
Current ACLs for resource `ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, patternType=LITERAL)`:
        (principal=User:CN=mirror-maker, host=*, operation=CREATE, permissionType=ALLOW)
        (principal=User:CN=kafka-ui, host=*, operation=DESCRIBE|DESCRIBE_CONFIGS, permissionType=ALLOW)
Current ACLs for resource `ResourcePattern(resourceType=GROUP, name=spark-kafka-source, patternType=PREFIXED)`:
        (principal=User:CN=analytics, host=*, operation=READ|DESCRIBE, permissionType=ALLOW)

== ACL успешно применены
```

(операции одного principal'а для краткости собраны в одну строку)

**Шаги 5–6. Elasticsearch, коннекторы, список запрещённых товаров:**

```text
==== 5/6 Хранилище: Elasticsearch и коннекторы
  ждём: Elasticsearch — готово (0 с)
  [+] роли marketplace_writer / marketplace_reader, пользователи kafka_connect / client_api
  [=] индекс client-requests уже существует
  [=] индекс orders уже существует
  [=] индекс products-filtered уже существует
  [=] индекс recommendations уже существует
  [ok] Kafka Connect REST API доступен (0 с)
  [+] Коннектор elasticsearch-sink зарегистрирован
  [+] Коннектор file-sink зарегистрирован
  [ok] elasticsearch-sink: RUNNING (0 с)
  [ok] file-sink: RUNNING (0 с)
  [+] Все коннекторы успешно запущены

==== 6/6 Начальный список запрещённых товаров
banned-products | Загрузка 3 позиций из data/banned-products.json
banned-products | Товар 90001 добавлен в список запрещённых (причина: Табачная и никотиносодержащая продукция)
banned-products | Товар 90002 добавлен в список запрещённых (причина: Оружие и его копии)
banned-products | Товар 90003 добавлен в список запрещённых (причина: Пиротехника классов F3-F4)
```

### `bash scripts/demo.sh`

Ниже прогон демо, выполненный **не первым** на этом стенде. Поэтому счётчики сообщений в итоге больше, чем дал бы один прогон, а в истории запросов есть записи прошлых прогонов.

**Шаги 1–3. SHOP API и потоковый фильтр.** Магазины отправили 20 товаров и одно намеренно битое сообщение. Faust пропустил 17 товаров, заблокировал 3 запрещённых, битое сообщение отправил в DLQ:

```text
==== 1. Текущий список запрещённых товаров
Запрещённых товаров: 3
  90001      Табачная и никотиносодержащая продукция       с 2026-11-24T18:35:04+00:00
  90002      Оружие и его копии                            с 2026-11-24T18:35:04+00:00
  90003      Пиротехника классов F3-F4                     с 2026-11-24T18:35:04+00:00

==== 2. SHOP API: магазины отправляют 20 товаров из data/products.json (+1 битое сообщение)
shop-api | Отправка 20 товаров из data/products.json в топик shop-products
shop-api | -> 12345 | Умные часы XYZ | магазин store_001
...
shop-api | -> 90003 | Фейерверк «Салют 100 залпов» | магазин store_009
shop-api | -> broken-1 | намеренно битое сообщение
shop-api | Итог: поставлено в очередь=21, подтверждено брокером=21, не доставлено=0, пропущено при валидации=0
  ждём: в Elasticsearch 17 разрешённых товаров — готово (0 с)

==== 3. Решения потокового фильтра (лог faust-moderation)
[INFO] [PASSED]  12347 | Фитнес-браслет Pulse 3
[INFO] [PASSED]  12348 | Беспроводные наушники SoundMax
...
[WARNING] [BLOCKED] 90002 | Пневматический пистолет Striker | магазин store_008 | причина: Оружие и его копии
[ERROR] [INVALID] key=broken-1: Expecting property name enclosed in double quotes: line 1 column 2 (char 1) -> shop-products-dlq
[INFO] [PASSED]  22004 | Гантели разборные PowerSet 20 кг
[WARNING] [BLOCKED] 90001 | Электронная сигарета VapeCloud | магазин store_008 | причина: Табачная и никотиносодержащая продукция
[WARNING] [BLOCKED] 90003 | Фейерверк «Салют 100 залпов» | магазин store_009 | причина: Пиротехника классов F3-F4
```

Порядок строк в логе Faust не совпадает с порядком отправки: топик состоит из 3 партиций, и порядок гарантируется только внутри партиции (для одного `product_id`).

**Шаг 4. CLIENT API: поиск.** Каждый запрос выполняется по Elasticsearch и записывается событием в топик `client-requests`. Запрещённый товар не находится:

```text
Поиск «умные часы»: найдено 2
 1. [12345] Умные часы XYZ
    Электроника | XYZ | 4999.99 RUB | в наличии: 150 | магазин: store_001
    Умные часы с функцией мониторинга здоровья, GPS и уведомлениями.
 2. [12346] Умные часы XYZ Pro
    Электроника | XYZ | 12999.0 RUB | в наличии: 60 | магазин: store_001
    Флагманские умные часы: ЭКГ, пульсоксиметр, AMOLED-экран, eSIM.
client-api | Событие 4044ea83-105c-44b5-adaf-c0fa47a0b52f (search) записано в топик client-requests

Поиск «книга kafka»: найдено 1
 1. [55001] Книга «Kafka: подробное руководство»
    Книги | Питер | 2190.0 RUB | в наличии: 500 | магазин: store_007

Поиск «электронная сигарета»: найдено 0
client-api | Событие 57dbbc7c-d40c-4811-aab8-0c5105e65f1e (search) записано в топик client-requests
```

**Шаг 5. Заказы и резерв на складе.** Заказы пишутся в топик `orders`, попадают в Elasticsearch, а Faust уменьшает остаток товара. Запрещённый товар купить нельзя:

```text
Заказ a77e88ba-b6f1-44b2-be4a-1381d69ec072 оформлен
  Кроссовки беговые AirRun × 2 = 14980.0 RUB
...
Товар 90001 не найден или недоступен для продажи.
  ждём: 4 заказа в Elasticsearch — готово (0 с)

Заказы пользователя user_002 (из Elasticsearch): 2
  2026-11-24T18:49:44+00:00 | a77e88ba-b6f1-44b2-be4a-1381d69ec072 | created | 14980.0 RUB | Кроссовки беговые AirRun × 2
  2026-11-24T18:42:57+00:00 | eda1b6bc-0a22-4994-bead-cc08ab51fe3d | created | 14980.0 RUB | Кроссовки беговые AirRun × 2
  Остаток товара 22001 после заказа 2 шт. (Faust учёл резерв):
  :{"stock":{"reserved":17,"available":88}}}
```

Магазин прислал по товару `22001` остаток `available=90, reserved=15`. После заказа на 2 шт. в хранилище `available=88, reserved=17`.

**Шаги 6–7. Второй кластер, HDFS, Spark.** MirrorMaker 2 скопировал данные, `spark-ingest` загрузил их в HDFS, Spark дождался, пока Data Lake догонит Kafka, и построил витрины:

```text
==== 6. Дублирование во второй кластер
  ждём: MirrorMaker 2 скопировал данные в analytics — готово (0 с)

==== 7. Аналитика: HDFS -> Spark -> топик recommendations
[analytics] Data Lake актуален: 9 партиций догнали Kafka
[analytics] товаров в каталоге: 17, событий клиентов: 18, строк заказов: 8

=== Каталог по категориям  ->  hdfs://namenode:8020/datalake/marts/catalog_by_category
|category       |products|avg_price|stock_available|
|Электроника    |6       |9243.16  |1109           |
|Спорт          |4       |5990.0   |367            |
|Бытовая техника|3       |17823.33 |214            |
|Книги          |2       |2390.0   |800            |
|Одежда         |2       |5740.0   |230            |

=== Продажи по категориям  ->  hdfs://namenode:8020/datalake/marts/sales_by_category
|category       |orders|units|revenue|buyers|
|Бытовая техника|2     |2    |59980.0|1     |
|Спорт          |4     |6    |40940.0|2     |
|Электроника    |2     |2    |9999.98|1     |

=== Самые продаваемые товары  ->  hdfs://namenode:8020/datalake/marts/top_products_sales
|product_id|name                            |category       |units|revenue|orders|
|22001     |Кроссовки беговые AirRun        |Спорт          |4    |29960.0|2     |
|33002     |Кофемашина BaristaHome          |Бытовая техника|2    |59980.0|2     |
|22004     |Гантели разборные PowerSet 20 кг|Спорт          |2    |10980.0|2     |
|12345     |Умные часы XYZ                  |Электроника    |2    |9999.98|2     |

=== Рекомендации (построчно)
|user_id |strategy         |rank|product_id|name                            |category       |price  |score|
|user_001|category_affinity|1   |12346     |Умные часы XYZ Pro              |Электроника    |12999.0|184.0|
|user_001|category_affinity|2   |12348     |Беспроводные наушники SoundMax  |Электроника    |6990.0 |182.0|
|user_002|category_affinity|1   |22004     |Гантели разборные PowerSet 20 кг|Спорт          |5490.0 |170.0|
|user_002|category_affinity|2   |22002     |Кроссовки трейловые AirRun Trail|Спорт          |8990.0 |162.0|
|user_006|popular          |1   |12345     |Умные часы XYZ                  |Электроника    |4999.99|14.0 |
...
[analytics] Записано 7 рекомендаций в топик recommendations основного кластера (kafka-1:19092,kafka-2:19092,kafka-3:19092)

==== Data Lake в HDFS
/datalake/marts/catalog_by_category
/datalake/marts/category_demand
/datalake/marts/recommendations
/datalake/marts/sales_by_category
/datalake/marts/top_products_sales
/datalake/marts/top_queries
/datalake/raw/topic=primary.client-requests/ingest_date=2026-11-24
/datalake/raw/topic=primary.orders/ingest_date=2026-11-24
/datalake/raw/topic=primary.products-filtered/ingest_date=2026-11-24
```

У `user_002` в рекомендациях нет кроссовок `22001`: он их уже купил. Пользователи `user_005` и `user_006` не искали и не покупали товары, поэтому получили популярные товары (стратегия `popular`).

**Шаг 8. CLIENT API: персональные рекомендации** (из топика `recommendations` через Connect и Elasticsearch):

```text
Рекомендации для user_001 (стратегия: category_affinity, рассчитаны 2026-11-24T18:50:22+00:00)
Интересы пользователя: Электроника
 1. [12346] Умные часы XYZ Pro | Электроника | 12999.0 RUB | score=184.0
 2. [12348] Беспроводные наушники SoundMax | Электроника | 6990.0 RUB | score=182.0
 3. [12349] Наушники-вкладыши SoundMax Mini | Электроника | 2990.0 RUB | score=182.0
 4. [12347] Фитнес-браслет Pulse 3 | Электроника | 2490.0 RUB | score=180.0
 5. [12350] Смартфон Nova 12 | Электроника | 24990.0 RUB | score=180.0

Последние запросы пользователя user_001 (из Elasticsearch): 5
  2026-11-24T18:50:31+00:00 | recommendations | - | найдено: 0
  2026-11-24T18:49:36+00:00 | search          | наушники | найдено: 2
  2026-11-24T18:49:35+00:00 | search          | умные часы | найдено: 2
```

У событий `recommendations` поле «найдено» равно 0: в событии фиксируется сам факт запроса рекомендаций, список товаров записывается только для поиска.

**Шаги 9–10. Запрет товара, который уже есть в хранилище, и снятие запрета.** Товар удаляется из Elasticsearch (tombstone), пропадает из поиска и рекомендаций, а после снятия запрета возвращается:

```text
==== 9. Запрет товара, который уже есть в хранилище
banned-products | Товар 12346 добавлен в список запрещённых (причина: Проверка: товар удаляется из хранилища)
  ждём: товар 12346 удалён из Elasticsearch. — готово (3 с)

Поиск «умные часы»: найдено 1
 1. [12345] Умные часы XYZ

Товар 12346 не найден или недоступен для продажи.

Рекомендации для user_001 (стратегия: category_affinity, рассчитаны 2026-11-24T18:50:22+00:00)
 2. [12348] Беспроводные наушники SoundMax | Электроника | 6990.0 RUB | score=182.0
 3. [12349] Наушники-вкладыши SoundMax Mini | Электроника | 2990.0 RUB | score=182.0
 4. [12347] Фитнес-браслет Pulse 3 | Электроника | 2490.0 RUB | score=180.0
 5. [12350] Смартфон Nova 12 | Электроника | 24990.0 RUB | score=180.0
(скрыто недоступных для продажи товаров: 1)

==== 10. Снятие запрета: Faust публикует последнюю версию товара снова
banned-products | Товар 12346 удалён из списка запрещённых
  ждём: товар 12346 вернулся в Elasticsearch — готово (0 с)
```

Рекомендации считаются пакетно, поэтому у оставшихся товаров сохраняется рейтинг из расчёта Spark (позиция 1 скрыта).

**Шаг 11. Итоговое состояние** (после нескольких прогонов демо):

```text
==== Основной кластер (primary): сообщений в топиках (последний офсет)
  shop-products          105
  products-filtered      101
  shop-products-dlq      5
  banned-products        8
  client-requests        25
  orders                 8
  recommendations        14

==== Аналитический кластер (analytics): копии MirrorMaker 2
  primary.shop-products        primary=105    analytics=105
  primary.products-filtered    primary=101    analytics=101
  primary.client-requests      primary=25     analytics=25
  primary.orders               primary=8      analytics=8

==== Фильтрация: ID товаров в Elasticsearch и запрещённые ID
12345 12346 12347 12348 12349 12350 22001 22002 22003 22004 33001 33002 33003 44001 44002 55001 55002
  запрещённые (data/banned-products.json): 90001 90002 90003

==== Хранилище: Elasticsearch
index             health docs.count
client-requests   green          24
products-filtered green          17
orders            green          16
recommendations   green          35

==== Хранилище: файл FileStream Sink (/data/sink/products-filtered.jsonl)
  строк: 94

==== Коннекторы
{"name":"elasticsearch-sink","connector":{"state":"RUNNING",...},"tasks":[{"id":0,"state":"RUNNING",...}],"type":"sink"}
{"name":"file-sink","connector":{"state":"RUNNING",...},"tasks":[{"id":0,"state":"RUNNING",...}],"type":"sink"}

==== DLQ потоковой обработки (shop-products-dlq)
{"error": "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)", "source_topic": "shop-products", "raw_value": "{not a valid json", "failed_at": "2026-11-24T18:49:33+00:00"}

==== Data Lake (HDFS)
18.3 K   18.3 K   /datalake/raw/_spark_metadata
22.9 K   68.7 K   /datalake/raw/topic=primary.client-requests
16.3 K   49.0 K   /datalake/raw/topic=primary.orders
100.8 K  302.4 K  /datalake/raw/topic=primary.products-filtered
14.8 K   44.3 K   /datalake/marts/recommendations
```

Как читать эти числа:
- **Копии в analytics совпадают с оригиналами** по всем четырём топикам, отставание MirrorMaker 2 равно 0.
- **В Elasticsearch ровно 17 товаров**, запрещённых `90001–90003` среди них нет.
- В `products-filtered` 101 сообщение: кроме товаров там обновления остатков после заказов и tombstone при запретах.
- `docs.count` в `_cat/indices` учитывает и вложенные (`nested`) документы. В `orders` 8 заказов по одной позиции, то есть 8 + 8 = 16. В `recommendations` 7 пользователей и 28 рекомендованных товаров, то есть 7 + 28 = 35.
- В файле 94 строки, меньше чем сообщений в топике: tombstone в файл не пишутся (SMT `Filter`).
- Lag всех групп `faust-moderation` по бизнес-топикам равен 0.

### Мониторинг (Grafana)

Дашборд «Kafka — платформа «Покупай выгодно»», http://localhost:3000.

Состояние кластера: 3 из 3 брокеров онлайн, один активный контроллер KRaft, нет under-replicated, under-min-ISR и offline-партиций. Лидеры распределены по брокерам равномерно (39 / 42 / 38).

![Состояние кластера primary](<screenshots/Снимок экрана 2026-11-24 231420.png>)

Трафик: входящие сообщения по топикам, байты in/out и трафик репликации по брокерам, запросы по типам, задержки p50/p99.

![Трафик](<screenshots/Снимок экрана 2026-11-24 231429.png>)

Репликация и дублирование: ISR shrink/expand, входящие сообщения аналитического кластера от MirrorMaker 2, размер топиков, число групп потребителей.

![Репликация и дублирование](<screenshots/Снимок экрана 2026-11-24 231437.png>)

Ресурсы JVM: heap брокеров и простой обработчиков запросов и сетевых потоков.

![Ресурсы JVM](<screenshots/Снимок экрана 2026-11-24 231446.png>)

Потребители, MirrorMaker 2, Connect, Faust: 3 брокера analytics онлайн, 2 коннектора RUNNING, 0 упавших задач, lag потребителей, отставание MirrorMaker 2 (0 по всем топикам), решения потокового фильтра, сообщения в топиках.

![Потребители, MirrorMaker 2, Connect, Faust](<screenshots/Снимок экрана 2026-11-24 231454.png>)

---

## Структура проекта

```
.
├── start.sh / stop.sh / cli.sh          # запуск, остановка, CLI-команды
├── docker-compose.yml                   # вся платформа (26 сервисов)
├── .env.example                         # пароли, BIND_ADDRESS, Telegram (копируется в .env)
├── app/marketplace/                     # Python-сервисы
│   ├── shop_api.py                      #   SHOP API (эмуляция)
│   ├── client_api.py                    #   CLIENT API: search / recommend / buy / orders / history
│   ├── banned_cli.py                    #   CLI списка запрещённых товаров
│   ├── moderation.py                    #   Faust: фильтр, tombstone, резервы, метрики
│   ├── alert_webhook.py                 #   приёмник алертов Alertmanager
│   ├── kafka_client.py, config.py       #   mTLS-подключение, общие настройки
├── analytics/                           # Spark: ingest_to_hdfs.py (поток), recommendations.py (batch)
├── data/                                # products.json (данные магазинов), banned-products.json
├── config/
│   ├── connect/                         # конфиги коннекторов
│   ├── elasticsearch/                   # маппинги индексов
│   ├── mirror-maker/                    # MirrorMaker 2
│   └── hadoop/                          # HDFS
├── docker/                              # Dockerfile: kafka (+JMX), connect (+ES Sink 15.0.0, JMX), app
├── monitoring/                          # JMX Exporter (брокеры, Connect), Prometheus (+алерты), Alertmanager, Grafana
├── screenshots/                         # скриншоты и логи запуска (см. «Результаты запуска»)
└── scripts/
    ├── common.sh                        # общие функции: доступ к кластерам, ES (curl / curl.exe), wait_until
    ├── generate-certs.sh                # CA и сертификаты (keystore/truststore)
    ├── kafka/init-topics-acl.sh         # топики и ACL primary (в контейнере kafka-tools)
    ├── kafka/init-analytics-acl.sh      # ACL analytics
    ├── kafka/check-security.sh          # проверки TLS/ACL (в контейнере kafka-tools)
    ├── init-elasticsearch.sh, register-connectors.sh
    ├── run-analytics.sh, demo.sh, check-*.sh, test-broker-failure.sh
```

---

## Решение проблем

- **`Нужен OpenSSL >= 3.2`**. В Windows необходимо запускать скрипты через свежий Git Bash. В Linux можно сгенерировать сертификаты в контейнере:
  `docker run --rm -v "$PWD":/w -w /w alpine:3.20 sh -c 'apk add -q bash openssl && bash scripts/generate-certs.sh'`.
- **Пароль `KEYSTORE_PASSWORD` поменяли после генерации сертификатов**. Необходимо выполнить `bash scripts/generate-certs.sh --force` и перезапустить стенд.
- **Пароли ES поменяли в `.env`**. Необходимо повторно выполнить `bash scripts/init-elasticsearch.sh` (обновит пользователей) и `docker compose up -d kafka-connect client-api-cli`.
- **Контейнеры падают по памяти (код 137)**. Необходимо увеличить память Docker до 16 ГБ и больше.
- **`$'\r': command not found`**. Скрипты попали в репозиторий с CRLF. В `.gitattributes` задан `eol=lf`; необходимо выполнить `git add --renormalize .`.
- **`curl: (23) Failure writing output to destination` в Git Bash**. Это ошибка встроенного `curl` Git Bash при выводе в `/dev/null`. `scripts/common.sh` и `scripts/register-connectors.sh` в Git Bash (MINGW/MSYS/CYGWIN) автоматически используют `curl.exe` из Windows и `NUL` вместо `/dev/null`.
- **Предупреждение `Failed to acquire idempotence PID ... Coordinator load in progress: retrying`** сразу после старта. Это не ошибка: брокер ещё загружает координатор транзакций, а идемпотентный продюсер librdkafka повторяет запрос сам. Сообщения доставляются, в итоге будет «не доставлено=0».
- **При первом запуске `demo.sh` не дождался товаров в Elasticsearch** (`не дождались за 120 с`). На свежем стенде Faust и Kafka Connect могут ещё не закончить первую ребалансировку групп потребителей. Необходимо подождать минуту и запустить `bash scripts/demo.sh` ещё раз: все шаги идемпотентны.
- **`docker compose up --wait` завершился ошибкой**. Необходимо посмотреть, какой сервис не прошёл healthcheck: `docker compose ps`, затем `docker compose logs <сервис>`.
- **Коннектор в FAILED**. Необходимо проверить статус и логи: `curl localhost:8083/connectors/elasticsearch-sink/status` и `docker compose logs kafka-connect`; на дашборде это алерт `KafkaConnectorNotRunning`.
- **Рекомендаций нет**. Необходимы события клиентов (`client search`, `client buy`), после них нужно выполнить `bash scripts/run-analytics.sh`. Если `spark-ingest` не работает, необходимо посмотреть его лог: `docker compose logs spark-ingest`.
