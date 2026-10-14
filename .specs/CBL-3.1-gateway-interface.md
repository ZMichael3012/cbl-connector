# CBL-3.1. Интерфейс PaymentGateway и каноническая модель

| Поле | Значение |
| :-- | :-- |
| Эпик | E3, Gateway Connector |
| Репозиторий | `cbl-connector` |
| Размер | M |
| Волна и параллельная группа | W1-CONNECTOR |
| Зависит от | 1.7 (CF-1), 1.4 |
| Версия контрактов | `cbl-protos@cf-1` (`billing/connector/v1`, `billing/events/v1`) |
| Статус | done (локально, ожидает коммита) |

## 1. Контекст и цель

Единый порт для внешних крипто-шлюзов и каноническая модель статусов: адаптеры (3.2, 10.3)
переводят провайдерские статусы и события в одну модель, доменные сервисы не знают о шлюзах
(`CBL-000`, раздел 2).

## 2. Границы

In scope: Go-интерфейс `PaymentGateway`, доменные модели, маппинг статусов, интерфейс роутера.
Out of scope: конкретные адаптеры (3.2), устойчивость (3.3), нормализатор Kafka (3.5).

## 3. Функциональные требования

FR-1. Интерфейс (internal/domain):

```go
type PaymentGateway interface {
    CreateInvoice(ctx context.Context, req CreateInvoiceRequest) (Invoice, error)
    GetPayment(ctx context.Context, gatewayID string) (PaymentStatus, error)
    VerifyWebhook(headers map[string][]string, rawBody []byte) (CanonicalEvent, error)
    CreatePayout(ctx context.Context, req PayoutRequest) (Payout, error)
    GetPayout(ctx context.Context, gatewayPayoutID string) (PayoutStatus, error)
    Capabilities() Capabilities
}
```

FR-2. Канонические модели (money только `cbl-kit/money.Money`; поля соответствуют
`billing/connector/v1` и `billing/events/v1`, актив `Invoice` выражается через `Amount.Asset`):
`Invoice{GatewayID, Provider, Address, Network, Amount, ExpiresAt}`;
`PaymentStatus{GatewayID, PaymentID, Provider, State, ReceivedAmount, Confirmations, TXID,
Network, GatewayEventTS}`; `CanonicalEvent{Type, PaymentID, GatewayID, Provider, TXID, Network,
ReceivedAmount, Confirmations, OccurredAt}`; `PayoutStatus{State, TXID, Fee, Reason}`;
`Capabilities{Provider, Networks map[string]NetworkCapability{Asset, MinAmount, ConfirmationDepth}}`.

FR-3. Канонические состояния: `detected`, `confirmed`, `failed`, `expired`, `underpaid`,
`overpaid`; выплата: `submitted`, `confirmed`, `failed`. Маппинг провайдерских строк в эти
состояния выполняет адаптер через helpers `CanonicalState(raw string) (State, error)` и
`CanonicalPayoutState(raw string) (PayoutState, error)`; таблицы псевдонимов остаются
внутренними, наружу экспортируются только функции маппинга.

FR-4. Роутер (интерфейс, реализация в 3.4):

```go
type Router interface {
    Select(ctx context.Context, req RouteRequest) (PaymentGateway, error)
}
type RouteRequest struct { Network, Asset string; Amount money.Money; Operation string }
```

FR-5. Домен возвращает sentinel-ошибки (`ErrUnknownState`, `ErrInvalidRoute`,
`ErrUnsupportedNetwork`, `ErrProviderUnavailable`); маппинг в `grpcmw.E` (not_found,
unavailable, invalid_argument) выполняется транспортным слоем задач 3.2/3.4. Таймауты и
ретраи не реализуются в этом слое (3.3).

FR-6. Локальные зависимости: `replace` на `../cbl-kit` (money) и `../cbl-protos` (добавлен
авансом, используется с задач 3.2/3.5).

## 4. Контракты

Типы сверяются с `billing/connector/v1` (`CanonicalPaymentStatus`, `Capability`) и
`billing/events/v1` (`CanonicalPaymentEvent`, `GatewayPayoutEvent`); маппинг 1:1, поля не
расширяются без обновления `cbl-protos`.

## 5. Модель данных и миграции

Хранилище адаптеров и логи вызовов - задача 3.4 (`connector` database); здесь миграции нет.

## 6. Конечные автоматы

Канонические состояния и запрет переходов не по порядку контролирует Payment (4.3); Connector
обязан публиковать события монотонно по `gateway_event_ts` и не скрывать повторы.

## 7. Edge cases

Неизвестный провайдерский статус (ошибка `invalid_argument`, событие не публикуется),
отсутствующие поля в ответе шлюза, расхождение суммы и актива, пустой TXID, дубликат события
(обрабатывается Payment/inbox), сеть не поддерживается провайдером.

## 8. Нефункциональные требования

Интерфейс не зависит от HTTP-клиента провайдера; unit-тесты без сети. Готовность к набору
контрактных тестов 3.7: тестовый двойник адаптера.

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Smoke | `go build ./...` |
| Unit | маппинг статусов всех состояний и ошибок, валидация RouteRequest, Capabilities |
| Integration | не применимо (появляется в 3.2) |
| E2E | не применимо |
| Load | не применимо |

## 10. План работ и коммит

Коммит `CBL-3.1: интерфейс PaymentGateway и каноническая модель`; спека в том же коммите.

## 11. Открытые вопросы

- Провайдер номер 1 не выбран (KI-01): адаптер 3.2 стартует после фиксации провайдера,
  разработка ведется на `cbl-mock-gateway` (3.6).

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (13 - 14.10.2026); blocker и major не найдены, замечания minor
закрыты правками спеки и репозитория.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | расхождение FR-2 с реализацией (`Asset` внутри `Amount`, `Provider` в статусах) | FR-2 приведена к контрактам `billing/connector/v1` и `events/v1` |
| Архитектуры | `VerifyWebhook` дублирует ответственность Webhook Ingress | граница зафиксирована: подпись проверяет Ingress, Connector доверяет `signature_verified` (3.5) |
| С контекстом | нет `replace` на `../cbl-protos`; домен не маппится в `grpcmw.E` | replace добавлен; FR-5 уточнена: маппинг выполняет транспорт 3.2/3.4 |
| Безопасности | замечаний по коду нет (нет внешних вызовов и секретов); зависимости обновлены | закрыто обновлением Go 1.26 и kit |

