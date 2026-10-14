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
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make vet          # go vet ./...
make lint         # golangci-lint в Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

## 5. Порты и окружение

HTTP `8050`, gRPC `9050`, метрики `9250`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

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

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-CONNECTOR-01 | `VerifyWebhook` объявлен в порту шлюза, хотя подпись проверяет Webhook Ingress | defense-in-depth и симметрия порта | контракт подписи дублируется у двух сервисов | границы зафиксированы: Ingress верифицирует, Connector доверяет `signature_verified` (3.5); секреты только через Vault |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-connector-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-connector-02 | Нет e2e и адаптера провайдера | сквозной путь проверяется только unit-тестами домена | задачи 3.2 - 3.7 | open |
| KI-connector-03 | Нет mTLS на gRPC-сервере | порт 9050 не публиковать за пределы доверенной сети | задача 0.6 | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-3.1 | 2026-10-14 | done, ревью закрыто (интерфейс PaymentGateway, канонические статусы, покрытие домена 100%) | `.specs/CBL-3.1-gateway-interface.md` | ожидает |

## 11. Runbook

Заполняется первой продуктовой задачей сервиса. Типовые аварии: недоступность зависимости,
рост ошибок, лаг обработки; действия: проверить health/metrics, логи с request_id, состояние
Kafka consumer group и outbox lag, при необходимости перезапустить сервис.
