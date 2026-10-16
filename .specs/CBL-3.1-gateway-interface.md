# CBL-3.1. PaymentGateway interface and canonical model

| Field | Value |
| :-- | :-- |
| Epic | E3, Gateway Connector |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W1-CONNECTOR |
| Depends on | 1.7 (CF-1), 1.4 |
| Contract version | `cbl-protos@cf-1` (`billing/connector/v1`, `billing/events/v1`) |
| Status | done (locally, awaiting commit) |

## 1. Context and goal

A single port for external crypto gateways and a canonical status model: adapters (3.2, 10.3)
translate provider statuses and events into one model, domain services know nothing about
gateways (`CBL-000`, section 2).

## 2. Boundaries

In scope: the Go interface `PaymentGateway`, domain models, status mapping, the router interface.
Out of scope: concrete adapters (3.2), resilience (3.3), the Kafka normalizer (3.5).

## 3. Functional requirements

FR-1. Interface (internal/domain):

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

FR-2. Canonical models (money only via `cbl-kit/money.Money`; fields correspond to
`billing/connector/v1` and `billing/events/v1`, the `Invoice` asset is expressed through `Amount.Asset`):
`Invoice{GatewayID, Provider, Address, Network, Amount, ExpiresAt}`;
`PaymentStatus{GatewayID, PaymentID, Provider, State, ReceivedAmount, Confirmations, TXID,
Network, GatewayEventTS}`; `CanonicalEvent{Type, PaymentID, GatewayID, Provider, TXID, Network,
ReceivedAmount, Confirmations, OccurredAt}`; `PayoutStatus{State, TXID, Fee, Reason}`;
`Capabilities{Provider, Networks map[string]NetworkCapability{Asset, MinAmount, ConfirmationDepth}}`.

FR-3. Canonical states: `detected`, `confirmed`, `failed`, `expired`, `underpaid`,
`overpaid`; payout: `submitted`, `confirmed`, `failed`. The adapter maps provider strings
into these states via the helpers `CanonicalState(raw string) (State, error)` and
`CanonicalPayoutState(raw string) (PayoutState, error)`; alias tables stay internal,
and only the mapping functions are exported.

FR-4. Router (interface, implementation in 3.4):

```go
type Router interface {
    Select(ctx context.Context, req RouteRequest) (PaymentGateway, error)
}
type RouteRequest struct { Network, Asset string; Amount money.Money; Operation string }
```

FR-5. The domain returns sentinel errors (`ErrUnknownState`, `ErrInvalidRoute`,
`ErrUnsupportedNetwork`, `ErrProviderUnavailable`); the mapping into `grpcmw.E` (not_found,
unavailable, invalid_argument) is performed by the transport layer of tasks 3.2/3.4. Timeouts
and retries are not implemented in this layer (3.3).

FR-6. Local dependencies: `replace` for `../cbl-kit` (money) and `../cbl-protos` (added
in advance, used from tasks 3.2/3.5).

## 4. Contracts

The types are verified against `billing/connector/v1` (`CanonicalPaymentStatus`, `Capability`) and
`billing/events/v1` (`CanonicalPaymentEvent`, `GatewayPayoutEvent`); the mapping is 1:1, and fields are
not extended without updating `cbl-protos`.

## 5. Data model and migrations

Adapter storage and call logs are task 3.4 (`connector` database); there is no migration here.

## 6. State machines

Payment controls canonical states and forbids out-of-order transitions (4.3); Connector
must publish events monotonically by `gateway_event_ts` and must not hide duplicates.

## 7. Edge cases

An unknown provider status (an `invalid_argument` error, the event is not published),
missing fields in the gateway response, an amount and asset mismatch, an empty TXID, a duplicate event
(handled by Payment/inbox), a network unsupported by the provider.

## 8. Non-functional requirements

The interface does not depend on the provider HTTP client; unit tests run without network. Readiness for the
contract test suite 3.7: a test double of the adapter.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...` |
| Unit | status mapping for all states and errors, RouteRequest validation, Capabilities |
| Integration | not applicable (appears in 3.2) |
| E2E | not applicable |
| Load | not applicable |

## 10. Work plan and commit

Commit `CBL-3.1: PaymentGateway interface and canonical model`; the spec is part of the same commit.

## 11. Open questions

- Provider number 1 is not chosen (KI-01): adapter 3.2 starts after the provider is fixed,
  development proceeds on `cbl-mock-gateway` (3.6).

## 12. Review (CBL-001, step 6)

Four independent passes (13 - 14.10.2026); no blocker or major findings were found, minor comments
were closed by edits to the spec and the repository.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | FR-2 diverged from the implementation (`Asset` inside `Amount`, `Provider` in statuses) | FR-2 was aligned with the `billing/connector/v1` and `events/v1` contracts |
| Architecture | `VerifyWebhook` duplicated the Webhook Ingress responsibility | the boundary is fixed: Ingress verifies the signature, Connector trusts `signature_verified` (3.5) |
| With context | no `replace` for `../cbl-protos`; the domain was not mapped into `grpcmw.E` | replace added; FR-5 clarified: the mapping is performed by the transport layer of tasks 3.2/3.4 |
| Security | no code findings (no external calls or secrets); dependencies updated | closed by the Go 1.26 and kit update |

