# CBL-4.2 (connector increment). Invoice replay destination

| Field | Value |
| :-- | :-- |
| Epic | E4/E17 (payment creation, CBL-4.2) |
| Repository | `cbl-connector` |
| Size | S |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (provider_invoices mapping, CreateInvoice replay) |
| Contract version | `cbl-protos@cf-4.2` (consumed, no change) |
| Status | done |

## 1. Context and goal

The idempotent replay of `CreateInvoice` (17.2) returned only the gateway
invoice id and the provider. The payment creation flow (CBL-4.2) relies on
that replay to finish after a lost response (E-3: the invoice exists at the
provider, the payment is in `created`), but without the destination the
payment could never move to `pending` and the merchant would never receive an
address. The mapping now stores the destination and the replay returns it.

## 2. Scope

In scope: migration 00009 (`provider_invoices.address`, `checkout_url`), the
domain mapping fields with validation bounds, the storage Save/scan, the
transport replay response, tests, docs.

Out of scope: provider-side invoice cancellation (needs a connector RPC,
KI-payment-09), address rotation and expiry re-reads.

## 3. Architecture sketch

`domain.ProviderInvoice` gains `Address` (<=256 printable) and `CheckoutURL`
(<=2048 printable); `storage.ProviderInvoicesRepository.Save` persists them,
`rowToDomain` returns them and the replay branch of `ConnectorServer.CreateInvoice`
fills the response from the stored mapping. The stored mapping is immutable per
(provider, gateway_id) as before: the destination belongs to the created
invoice and never changes.

## 4. Functional requirements

FR-1. A create that lost its response replays through the mapping and returns
the stored destination (address or checkout page) with the gateway id and
provider, so the caller can reach `pending`.

FR-2. Mappings without a destination (legacy rows, mock profiles without an
address) keep replaying without one; the caller decides whether that is
sufficient.

## 5. Contracts

No change: `CreateInvoiceResponse` already carries `address`/`checkout_url`
(cf-4.2 consumed).

## 6. Data model and migrations

Migration 00009 adds the two nullable bounded columns to `provider_invoices`.

## 7. Edge cases

E-1. Destination over the bounds: `Validate` rejects the save (the call fails
retryably as before).
E-2. A mapping saved before this increment has no destination: the replay
returns the ids only.

## 8. Test plan

Unit: the transport replay returns the stored checkout page
(`TestCreateInvoiceReplaysExistingOrder`). Integration: the destination
survives the round trip (`TestProviderInvoicesDestinationRoundTrip`,
testcontainers PostgreSQL).

## 9. Reviews

The change is small and additive; it was reviewed as part of the CBL-4.2
review round (the blocking finding "the connector replay returns no address"
is this increment's closure).

## 10. Documentation

`AGENTS.md` (journal row; the provider_invoices description in the architecture
section mentions the destination).
