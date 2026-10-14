# AGENTS.md - cbl-connector

Абстракция внешних крипто-шлюзов: адаптеры, роутинг, устойчивость, нормализация вебхуков.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: интерфейс PaymentGateway, адаптеры провайдеров, circuit breaker, нормализация событий.

Out of scope: другие сервисы платформы; взаимодействие только через контракты `cbl-protos`
(gRPC), Kafka и REST периметра (см. `CBL-000`, раздел 2).

## 2. Контракты

- REST: административный /healthz, /readyz
- gRPC: сервер ConnectorService; исходящие HTTPS к шлюзам
- Kafka продюсирует: gateway.events.canonical, gateway.payout.events
- Kafka потребляет: gateway.webhooks.raw, payout.commands
- Хранилище: PostgreSQL connector, Redis (маршруты, circuit breaker)

Контракты заморожены в `cbl-protos` (CF-1 для gRPC, CF-2/CF-3 для событий), см. `CBL-004`.

## 3. Архитектура

Слои: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Пакет `internal/gateway` - адаптеры и декораторы порта `PaymentGateway`: `Resilient`
(таймауты, ретраи с backoff, in-process circuit breaker на провайдера, bulkhead);
включается роутером 3.4 одним экземпляром на провайдера.
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers PostgreSQL)
make test-e2e     # сквозной сценарий нормализатора (testcontainers Kafka + PostgreSQL)
make smoke        # compose-стенд: healthz, миграции, старт конвейера
make vet          # go vet ./...
make lint         # golangci-lint в Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

## 5. Порты и окружение

HTTP `8050`, gRPC `9050`, метрики `9250`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`MIGRATIONS_DIR`, `KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP` (default `cbl-connector-webhooks`),
`CONNECTOR_SANDBOX_PROVIDERS` (default `mock`), `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.
Устойчивость провайдера (3.3, значения по умолчанию): `CONNECTOR_RETRY_MAX_ATTEMPTS` (3),
`CONNECTOR_RETRY_BASE_DELAY` (100ms), `CONNECTOR_RETRY_MAX_DELAY` (1s),
`CONNECTOR_BREAKER_FAILURE_THRESHOLD` (5), `CONNECTOR_BREAKER_TIMEOUT` (30s),
`CONNECTOR_BREAKER_HALF_OPEN_REQUESTS` (1), `CONNECTOR_BULKHEAD_MAX_CONCURRENT` (32),
`CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT` (перекрывает пер-методные таймауты).

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> архитектура -> контракты -> тесты вперед -> код ->
  ревью 4 агентами (без контекста, архитектура, контекст, безопасность - блокирующий) ->
  AGENTS.md -> финальные прогоны -> линтеры -> коммит.
- Покрытие unit+integration не ниже 80%; денежные сценарии только на реальных PostgreSQL и
  Kafka (testcontainers); локальный e2e в docker-compose обязателен.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен.
- Docstring на русском в стиле godoc (первая строка с имени сущности, точка в конце);
  комментарии объясняют почему; ошибки через `%w`; панки в бизнес-коде запрещены.
- Коммит: `CBL-<эпик>.<номер>: <краткое сообщение>`, спека с тем же префиксом в том же коммите.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Базовые метрики: latency p95/p99, доля ошибок, outbox lag,
consumer lag; дашборды после задачи 0.7.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы | Компенсация |
| :-- | :-- | :-- | :-- | :-- |
| T-CONNECTOR-01 | `VerifyWebhook` объявлен в порту шлюза, хотя подпись проверяет Webhook Ingress | defense-in-depth и симметрия порта | контракт подписи дублируется у двух сервисов | границы зафиксированы: Ingress верифицирует, Connector доверяет `signature_verified` (3.5); секреты только через Vault |
| T-CONNECTOR-02 | До выбора провайдера (KI-01) работает sandbox-декодер JSON вместо адаптеров | конвейер нормализации разблокирован и покрыт e2e без реального провайдера | формат вебхука - наш контракт, не проверен на живом шлюзе | формат зафиксирован в CBL-3.5, реализуется mock-gateway (3.6); адаптеры 3.2 включаются через реестр декодеров |
| T-CONNECTOR-03 | `payment_id` берется из тела вебхука (sandbox) | простота, нет обратного маппинга | реальные провайдеры не знают payment_id платформы | адаптер 3.2 вернет payment_id через маппинг gateway_id; порт `WebhookDecoder` это допускает |
| T-CONNECTOR-04 | Повторные попытки inbox публикуются в retry-топики без отложенной доставки (механика kit) | простой конвейер, повторы не теряются | окно повторов практически нулевое: транзиентный сбой БД выгружает бэклог в DLQ за миллисекунды | усечение x-error и ограничение полей не дают poison pill заблокировать партицию; отложенные повторы и парковка - 14.2 (KI-connector-05) |
| T-CONNECTOR-05 | Порядок событий по `gateway_event_ts` контролирует Payment (4.3), нормализатор только пробрасывает время | нет состояния порядка в коннекторе | при retry событие может быть опубликовано с опозданием | требование зеркалируется в спеку Payment 4.3; ключ партиции payment_id сохраняет порядок одного платежа в норме |
| T-CONNECTOR-06 | Circuit breaker in-process (gobreaker на провайдера), ключ `cb:{provider}` зарезервирован | нет Redis на денежном пути, потеря состояния безопасна, нет ложных общих размыканий при частичной деградации | при I репликах фактический порог размыкания растет пропорционально, half-open-пробы множатся | консолидация общего стейта - при появлении мультиинстансного требования (KI-connector-07); пороги конфигурируемы |
| T-CONNECTOR-07 | Неудачей breaker считаются только временные сбои и таймауты; бизнес-отказы и отмена вызывающего не учитываются; пустой `idempotency_key` - fail-fast; конфигурация нормализуется с клэмпами | поток отвергнутых адресов и баги вызывающего не блокируют здорового провайдера; повтор не создает второй счет/выплату; «безумные» env не отключают защиту | адаптеры обязаны классифицировать ошибки (`ErrTemporary`/`ErrRejected`) и санитизировать их текст | классификация и маппинг зафиксированы в CBL-3.3 (§4) и CBL-003; контрактные тесты классификации - 3.7 |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-connector-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-connector-02 | Нет адаптера реального провайдера (KI-01) | нормализация проверена sandbox-декодером и e2e на testcontainers, не живым шлюзом | задача 3.2 после фиксации провайдера | open |
| KI-connector-03 | Нет mTLS на gRPC-сервере | порт 9050 не публиковать за пределы доверенной сети | задача 0.6 | open |
| KI-connector-04 | Нет очистки опубликованных outbox (канонический payload) и старых записей inbox; сырые тела вебхуков живут в outbox webhook-ingress и Kafka retry/DLQ-топиках | таблицы и топики растут, включая чувствительные заголовки провайдера | retention-задача (0.4/14.x), ACL/retention DLQ (0.5/5.3) | open |
| KI-connector-05 | Kit inbox: `x-error` не усекается, сбой публикации retry/DLQ не паркуется, повторы без задержки | при гигантских/битых событиях возможен шторм редоставки; в 3.5 ограничено лимитами полей и размера тела | усиление kit (усечение ошибок, parking, задержки) - задачи 1.3-ревизия/14.2 | open |
| KI-connector-06 | Kafka в compose - PLAINTEXT без SASL/ACL: запись в gateway.webhooks.raw не ограничена Ingress; Connector доверяет `signature_verified` | подмена флага подписи при доступе к топику обходит верификацию | SASL/TLS и ACL на запись в raw-топик - задача 0.5; алерт на несоответствие producer - 14.4 | open |
| KI-connector-07 | Состояние circuit breaker in-process: при нескольких репликах порог размыкания и half-open-пробы не согласованы; Redis-ключ `cb:{provider}` зарезервирован | при мультиинстансе возможны лишние вызовы к деградирующему провайдеру | консолидировать общий стейт при появлении мультиинстансного требования (T-CONNECTOR-06); пороги конфигурируемы | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-3.1 | 2026-10-14 | done, ревью закрыто (интерфейс PaymentGateway, канонические статусы, покрытие домена 100%) | `.specs/CBL-3.1-gateway-interface.md` | d105801 |
| CBL-3.3 | 2026-10-14 | done, ревью закрыто (таймауты, backoff-ретраи, breaker с предикатом неудачи, bulkhead; покрытие gateway 93.3%) | `.specs/CBL-3.3-resilience.md` | этот коммит |
| CBL-3.5 | 2026-10-14 | done, ревью закрыто (конвейер нормализации, sandbox-декодер, лимиты недоверенного входа, e2e Kafka+PG с poison-pill сценарием, покрытие 93.1%) | `.specs/CBL-3.5-normalizer.md` | 04aac61 |

## 11. Runbook

Конвейер нормализации:

1. Лаг: метрики consumer lag (0.7), логи `webhook normalizer started`, `canonical event queued in outbox`.
2. DLQ: топик `gateway.webhooks.raw.dlq`, причина в заголовке `x-error`; частые причины -
   неподписанный вебхук, неизвестный провайдер, битое тело; разбор вручную.
3. Outbox lag: логи relay `outbox published batch`, `outbox publish batch failed`;
   при недоступности Kafka события остаются в таблице и публикуются позже.
4. Отказ Redis не влияет на конвейер (Redis используется задачей 3.4).

Устойчивость провайдера (3.3):

1. Логи `provider call retry` (операция, попытка, задержка) и `provider breaker state
   changed` - частое размыкание указывает на деградацию провайдера или на неклассифицированные
   ошибки адаптера (проверить, что бизнес-отказы оборачиваются `ErrRejected`).
2. `ErrProviderOverloaded`: исчерпан bulkhead - провайдер получил слишком много
   параллельных операций; проверить `CONNECTOR_BULKHEAD_MAX_CONCURRENT` и залипшие вызовы.
3. `ErrProviderUnavailable` без записи breaker в логе: исчерпаны попытки; смотреть причину
   в цепочке ошибки (таймаут попытки, `ErrTemporary`). При множестве реплик помнить про
   KI-connector-07 (состояние breaker не общее).
