# CBL-3.5. Нормализатор вебхуков в канонические события

| Поле | Значение |
| :-- | :-- |
| Эпик | E3, Gateway Connector |
| Репозиторий | `cbl-connector` |
| Размер | M |
| Волна и параллельная группа | W1-CONNECTOR |
| Зависит от | 3.1, 1.3 |
| Версия контрактов | `cbl-protos@cf-1` (`billing/events/v1`) |
| Статус | done (ревью закрыто, коммит CBL-3.5) |

## 1. Контекст и цель

Асинхронный контур приема платежа (`CBL-000`, раздел 4): Webhook Ingress публикует сырой
вебхук в `gateway.webhooks.raw`, Connector нормализует его в `gateway.events.canonical`
для Payment (4.3) и Reconciler (9.1). Задача строит конвейер нормализации: идемпотентный
консьюмер (inbox 1.3), порт декодера провайдера, публикация через outbox. Реальные
провайдерские декодеры придут с адаптерами (3.2, 10.3); до выбора провайдера (KI-01)
работает универсальный sandbox-декодер для mock-шлюза (3.6).

## 2. Границы

In scope: консьюмер `gateway.webhooks.raw` (+ retry-топики), порт `WebhookDecoder` и реестр,
sandbox-декодер JSON, валидация канонического события, публикация через outbox, миграции
outbox/inbox, wiring в main, сквозной тест на реальных Kafka и PostgreSQL.

Out of scope: реальные адаптеры провайдеров (3.2), роутер (3.4), mock-gateway как сервис
(3.6, реализует формат вебхука), нормализация выплат (10.3), метрики Prometheus (0.7).

## 3. Функциональные требования

FR-1. Консьюмер: топик `gateway.webhooks.raw` и топики повторной доставки
`.retry.5s/.retry.1m/.retry.10m` (kit Router), consumer group `KAFKA_CONSUMER_GROUP`
(default `cbl-connector-webhooks`), ручной коммит офсетов после успешной обработки
(`kafkaconsumer`), дедупликация по `(consumer, event_id)` через inbox. Ошибка обработки
переводит сообщение в retry-топик, после исчерпания попыток - в `.dlq` с причиной
в заголовке `x-error` (поведение kit Processor). Сообщение без `event_id` уходит в DLQ.

FR-2. Обработчик нормализации (`inbox.Handler`, выполняется в транзакции guard):
- отклоняет конверт больше 512 КБ и тело вебхука больше 256 КБ до разбора: недоверенный
  вход не должен раздувать retry/DLQ-сообщения и блокировать партицию (poison pill);
- разбирает `GatewayWebhookRaw` из payload (ошибка разбора - `ErrInvalidWebhook`);
- проверяет `signature_verified`: false - `ErrUnsignedWebhook` (в DLQ после retries);
  граница с Webhook Ingress зафиксирована в T-CONNECTOR-01: подпись проверяет Ingress,
  Connector доверяет флагу;
- ищет декодер по `provider` в реестре; неизвестный провайдер - `ErrUnknownProvider`;
- `decoder.Decode` возвращает `domain.CanonicalEvent`; событие валидируется:
  `payment_id` - UUID, `gateway_id` и `provider` непустые printable ASCII до 128/64,
  известный канонический тип, ненулевой `occurred_at`, `txid` до 256 (пусто допустимо),
  `network` до 64 из charset кода (пусто допустимо), `received_amount` с активом 2 - 16
  символов charset, decimals не больше 18, неотрицательная сумма; ошибки не интерполируют
  недоверенные значения (текст ошибки и `x-error` остаются ограниченными);
- собирает `CanonicalPaymentEvent` (`billing/events/v1`) и конверт outbox:
  topic `gateway.events.canonical`, key `payment_id`, event_type `gateway.payment.<type>`,
  заголовки producer `cbl-connector`, schema_version `v1`, `provider`, `gateway_id` и
  проброс `traceparent` из входящего конверта только при валидном формате W3C;
  `occurred_at` - время события у провайдера;
- записывает конверт в outbox в той же транзакции, что и inbox-запись: событие не
  публикуется без отметки обработки и наоборот.

FR-3. Порт декодера и реестр:

```go
type WebhookDecoder interface {
    Provider() string
    Decode(ctx context.Context, raw *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error)
}
```

Реестр строится из списка декодеров, дубликат провайдера - ошибка конфигурации;
`Decoder(provider)` возвращает декодер и признак наличия. Порт покрывает платежные вебхуки:
адаптеры 3.2 реализуют его, для payout-событий (10.3) появится отдельный порт/конвейер;
реестр не зависит от конкретных провайдеров.

FR-4. Sandbox-декодер (до KI-01): тело вебхука - JSON контракта:

```json
{
  "payment_id": "01a138...", "gateway_id": "inv-1",
  "status": "confirmed",
  "received_amount": {"minor_units": 100000, "asset": "USDT", "decimals": 2},
  "confirmations": 3, "txid": "0x...", "network": "trc20",
  "event_ts": "2026-10-14T12:00:00Z"
}
```

`status` переводится через `domain.CanonicalState` (псевдонимы провайдерских строк 3.1);
пустой или длиннее 32 символов статус - `ErrInvalidWebhook`, неизвестный непустой -
`ErrUnknownState`; `payment_id` обязан быть UUID; отсутствующие обязательные поля,
отрицательная сумма, `event_ts` длиннее 64 символов или не парсируемый - `ErrInvalidWebhook`
(текст ошибки не содержит входное значение). Провайдеры включаются env
`CONNECTOR_SANDBOX_PROVIDERS` (default `mock`, пустой список - отказ старта);
формат фиксируется и используется mock-gateway (3.6) как контракт sandbox-провайдера.

FR-5. Ошибки домена: `ErrUnsignedWebhook`, `ErrUnknownProvider`, `ErrInvalidWebhook`
(дополняют 3.1); mappping в gRPC не требуется: конвейер асинхронный, класс ошибки
определяет retry/DLQ через inbox.

FR-6. Wiring (main): `PG_DSN` обязателен (inbox/outbox), миграции через `storagex`;
при непустом `KAFKA_BROKERS` запускаются outbox relay (kit) и консьюмер нормализации;
без Kafka сервис работает без пайплайна (WARN), как в 5.1. Остановка по SIGTERM:
отмена контекста, закрытие консьюмера и publisher.

FR-7. E2E: сквозной сценарий (сырой вебхук в Kafka -> канонический в Kafka, дедупликация,
DLQ для неподписанного) выполняется на testcontainers с реальными Kafka и PostgreSQL
(build tag `e2e`, прецедент CBL-5.1 раздел 9); compose-смоук (`test/e2e/smoke.ps1`):
миграции применены, healthz ok, консьюмер запущен; топики создаются одноразовым сервисом
`kafka-init` в compose (auto-create выключен).

## 4. Контракты

- Kafka: `gateway.webhooks.raw` (вход, ключ `provider+gateway_id`, payload
  `GatewayWebhookRaw`), `gateway.events.canonical` (выход, ключ `payment_id`, payload
  `CanonicalPaymentEvent`); заголовки конверта из `CBL-003` (раздел 6); retry/DLQ-суффиксы
  kit. `cbl-protos@cf-1` без изменений.
- PostgreSQL `connector`: таблицы `outbox`, `inbox` (reference DDL из `cbl-kit`).
- gRPC/HTTP: без изменений (ConnectorService - 3.2/3.4).

## 5. Модель данных и миграции

`migrations/00001_outbox.sql`, `migrations/00002_inbox.sql` - копии reference DDL из
`cbl-kit/migrations` (`CBL-003`, раздел 7). Схема адаптеров и маршрутов - 3.4.

## 6. Конечные автоматы и правила

Состояния платежа ведет Payment (4.3); нормализатор не фильтрует повторные статусы и не
переупорядочивает события: он пробрасывает `gateway_event_ts` провайдера, а контроль
порядка (watermark по `gateway_event_ts` на платеж) - обязанность Payment (T-CONNECTOR-05,
требование зеркалируется в спеку 4.3). Дедупликация повторной доставки одного raw-события -
inbox по `event_id` (детерминированный UUIDv5 из 5.1), поэтому одно сырое событие дает
ровно один конверт canonical.

## 7. Edge cases

Дубликат raw-события (тот же `event_id`) - пропуск без публикации; неподписанный вебхук -
DLQ; неизвестный провайдер, неизвестный статус, битый protobuf/JSON, пустой или не-UUID
`payment_id` - retry, затем DLQ; poison pill (гигантские `status`/`event_ts`/тело,
конверт больше 512 КБ) отклоняется короткой ошибкой до парсинга, retry-сообщение остается
доставляемым и уходит в DLQ, партиция не блокируется; `event_ts` в будущем (принимается,
порядок решает Payment); пустой `txid` при detected; нулевые подтверждения; повторная
публикация relay (at-least-once, дедуплицирует Payment); активы и суммы вне лимитов
отклоняются валидацией (asset 2 - 16, decimals до 18, minor units неотрицательны).

## 8. Нефункциональные требования

- At-least-once без потерь: офсеты коммитятся после обработки; публикация только через
  outbox (нет dual-write).
- Обработка одного события не делает сетевых вызовов и укладывается в единицы
  миллисекунд (без вызовов провайдера); бюджет и лаг - 14.1.
- Логи не содержат сырых тел вебхуков и секретов; provider/gateway_id/payment_id в логах
  допустимы.
- Покрытие unit+integration не ниже 80%; денежные сценарии - на реальных PostgreSQL
  и Kafka (testcontainers).

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Smoke | `go build ./...`; compose: healthz, миграции, старт консьюмера (`test/e2e/smoke.ps1`) |
| Unit | реестр (дубликат, поиск, отсутствие), sandbox-декодер (валидный JSON, псевдонимы статусов, битый JSON/ts, пустой payment_id, отрицательная сумма), валидация события |
| Integration | testcontainers PostgreSQL: обработчик через inbox.Processor: канонический конверт в outbox, дубликат не публикуется, неподписанный/неизвестный/битый - ошибка для retry |
| E2E | testcontainers Kafka+PostgreSQL (tag `e2e`): raw -> canonical, дедупликация, DLQ неподписанного |
| Load | 14.1 |

## 10. План работ и коммит

1. Спека, тесты (unit + integration + e2e), затем реализация: ошибки домена, реестр,
   normalizer, sandbox-декодер, миграции, wiring, compose, smoke-скрипт.
2. Четыре ревью-прохода, AGENTS.md, линтеры, финальные прогоны.
3. Коммит `CBL-3.5: нормализатор вебхуков в канонические события`; спека в том же коммите.

## 11. Открытые вопросы и решения

- ADR: до выбора провайдера (KI-01) реестр наполняется sandbox-декодером; реальные
  декодеры - часть адаптеров 3.2, конвейер от них не зависит.
- ADR: событие без `signature_verified=true` не нормализуется: неподписанный вебхук не
  должен запускать денежные переходы; DLQ сохраняет сообщение для разбора.
- ADR: недоверенный вход ограничен до парсинга (конверт 512 КБ, тело 256 КБ, длины и
  charset полей, UUID payment_id): poison pill не блокирует партицию, x-error и логи
  ограничены; усиление kit (усечение x-error, parking, задержки повторов) - KI-connector-05.
- ADR: `producer` конверта не проверяется (KI-connector-06): граница доверия - ACL на
  запись в `gateway.webhooks.raw` в задаче 0.5; риск зафиксирован.
- Открытый вопрос: формат sandbox-вебхука фиксируется здесь; 3.6 реализует отправку,
  расхождения допустимы только через обновление спеки.
- Открытый вопрос: real-provider маппинг gateway_id -> payment_id в адаптере (3.2) может
  отличаться от sandbox (payment_id в теле); контракт порта это допускает.
- Открытый вопрос: Schema Registry не подключен (платформенное отставание 0.5): события
  публикуются plain protobuf, как и в 5.1; согласование с Payment (4.3) - вход волны W2.
- Принято с ограничением: при нескольких репликах каждая запускает свой relay; порядок
  событий одного платежа может нарушиться, Payment дедуплицирует и контролирует порядок
  (T-CONNECTOR-05); один relay на outbox - кандидат 14.2.

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (14.10.2026). Blocker безопасности (poison pill с гигантскими
`status`/`event_ts` блокировал партицию бесконечной редоставкой) закрыт лимитами
недоверенного входа и санитизацией ошибок; e2e дополнен poison-pill сценарием. Все major
и minor закрыты или зафиксированы.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | нет предела длины актива; дедупликация проверялась только негативным ожиданием; не покрыты future event_ts/пустой txid/битый protobuf; smoke без положительной проверки миграций; журнал опережал ревью; пустой список провайдеров; nil-реестр | лимиты и charset полей в Validate; e2e утверждает count outbox/inbox в БД; добавлены кейсы (пустой статус, длинные поля, poison); smoke проверяет `successfully migrated`; статусы синхронизированы; пустой список - отказ старта; требование к registry задокументировано |
| Архитектуры | монотонность occurred_at не реализована; retry без задержки выгружает бэклог в DLQ; os.Exit и graceful shutdown; readyz всегда 200; выплатный порт в декодере; несколько relay и порядок; отсутствие event_type ломает retry; Schema Registry | T-CONNECTOR-05: порядок - обязанность Payment (4.3); T-CONNECTOR-04 переформулирован честно, задержки/парковка - 14.2; exit/shutdown/readyz - платформенные задачи 0.7/14.4; FR-3 ограничена платежными вебхуками, payout-порт - 10.3; ограничение relay зафиксировано; kit-замечания в KI-connector-05; SR - 0.5 |
| С контекстом | заголовок trade-offs не совпадал со строками; KI-connector-04 неточен; дубль package-комментария; `%v` вместо `%w`; пустой status = ErrUnknownState; глобальный compose не ждал kafka-init | таблица приведена к 5 колонкам; KI уточнен; комментарий убран; `%w`; пустой status - ErrInvalidWebhook; глобальный compose ждет kafka-init |
| Безопасности (BLOCK снят) | poison pill блокирует партицию (гигантские поля в x-error); нет лимитов полей/формата payment_id; Kafka PLAINTEXT без ACL доверяет signature_verified; traceparent без валидации; retention DLQ | лимиты конверта/тела/полей, UUID payment_id, короткие ошибки без значений; e2e poison-pill сценарий подтверждает разблокировку; KI-connector-06 (ACL/SASL - 0.5); валидация W3C traceparent; KI-connector-04 (retention) |
