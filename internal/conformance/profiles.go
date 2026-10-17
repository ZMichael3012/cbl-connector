package conformance

import (
	"fmt"
	"time"

	"github.com/cbl/cbl-connector/internal/domain"
)

// StatusCase maps one documented raw status to the expected canonical state.
type StatusCase struct {
	// Raw sets the provider raw status.
	Raw string
	// Expected sets the canonical state the adapter must map it to.
	Expected domain.State
}

// ExpectedEvent describes the canonical event of the valid webhook fixture.
type ExpectedEvent struct {
	// GatewayID sets the provider invoice id of the event.
	GatewayID string
	// RawStatus sets the raw status of the event.
	RawStatus string
	// Type sets the canonical event type.
	Type domain.State
}

// ProfileFixtures describes one provider profile for the suite: the wire
// routes and payloads of the mock-gateway profile mirrored as test fixtures
// (mirror of the CBL-17.5 mock spec section 3.2; drift is detected by the
// sandbox runs 17.16).
type ProfileFixtures struct {
	// Provider sets the manifest id of the profile.
	Provider string
	// CreatePath sets the "METHOD /path" of the invoice creation route.
	CreatePath string
	// CreateStatus sets the response status of the creation route (201 default).
	CreateStatus int
	// CreateTemplate sets the response body; {{request.<dotted.path>}} outside
	// quotes echoes request values.
	CreateTemplate []byte
	// CreateGatewayID sets the invoice id embedded in the creation response.
	CreateGatewayID string
	// OrderField sets the dotted path of the order reference in the create request.
	OrderField string
	// CheckNetwork sets the network used by the CONF checks for this provider
	// (empty means the shared default "usdt_trc20"); providers with their own
	// network codes (NOWPayments "usdttrc20") set it so the adapter network
	// validation matches the configured capability row.
	CheckNetwork string
	// PaymentPath sets the "METHOD /path" of the payment route ("{gateway_id}" wildcard).
	PaymentPath string
	// PaymentGatewayID sets the invoice id embedded in the payment payloads.
	PaymentGatewayID string
	// PaymentByStatus maps raw statuses to payment payloads.
	PaymentByStatus map[string][]byte
	// PaymentUnknown sets the payment payload with an undocumented status.
	PaymentUnknown []byte
	// LookupPath sets the "METHOD /path?key={order}" lookup route (empty if absent).
	LookupPath string
	// PayoutPath sets the "METHOD /path" of the payout route (empty if absent).
	PayoutPath string
	// PayoutTemplate sets the payout response body.
	PayoutTemplate []byte
	// PayoutGatewayID sets the payout id embedded in the payout response.
	PayoutGatewayID string
	// PayoutExecutionPath sets the second payout call of two-step providers
	// (the BTCPay payout against a pull payment; empty for single-call providers).
	PayoutExecutionPath string
	// PayoutExecutionTemplate sets the second payout response body.
	PayoutExecutionTemplate []byte
	// PayoutExecutionGatewayID sets the payout id of the second call.
	PayoutExecutionGatewayID string
	// Secret sets the webhook signing secret of the profile.
	Secret []byte
	// WebhookValid sets the valid webhook body.
	WebhookValid []byte
	// WebhookHeaders sets the signature headers of the valid webhook.
	WebhookHeaders map[string]string
	// WebhookTampered sets the tampered webhook body.
	WebhookTampered []byte
	// TamperedHeaders sets the signature headers of the tampered webhook.
	TamperedHeaders map[string]string
	// WebhookMissing sets the unsigned webhook body of the missing-signature
	// probe: for header strategies it equals WebhookValid (the probe sends no
	// headers); for token echo it is the body with the token field removed
	// (mirror of the mock SignatureMissing mode).
	WebhookMissing []byte
	// ExpectedEvent sets the canonical event of the valid webhook.
	ExpectedEvent ExpectedEvent
	// Statuses lists the documented raw statuses and their canonical mapping.
	Statuses []StatusCase
}

// Fixtures returns the fixture set of a provider.
func Fixtures(provider string) (ProfileFixtures, bool) {
	switch provider {
	case "nowpayments":
		return nowPaymentsFixtures(), true
	case "coingate":
		return coinGateFixtures(), true
	case "bitpay":
		return bitPayFixtures(), true
	case "btcpay":
		return btcPayFixtures(), true
	case "mock":
		return mockFixtures(), true
	default:
		return ProfileFixtures{}, false
	}
}

// manifestOf returns the built-in manifest of a provider.
func manifestOf(provider string) domain.ProviderManifest {
	for _, manifest := range domain.BuiltinManifests() {
		if manifest.ID == provider {
			return manifest
		}
	}
	return domain.ProviderManifest{}
}

// signHeaders signs the valid webhook body per the manifest strategy; token
// echo profiles embed the token in the body and add no headers.
func signHeaders(provider string, secret, body []byte) map[string]string {
	header, value, err := SignBody(manifestOf(provider), secret, body)
	if err != nil {
		return map[string]string{}
	}
	return map[string]string{header: value}
}

// tamperedHeaders corrupts the signature value of a valid webhook.
func tamperedHeaders(valid map[string]string) map[string]string {
	tampered := make(map[string]string, len(valid))
	for header, value := range valid {
		if value == "" {
			tampered[header] = value
			continue
		}
		replacement := byte('0')
		if value[0] == '0' {
			replacement = '1'
		}
		tampered[header] = string(replacement) + value[1:]
	}
	return tampered
}

// paymentPayloads builds the per-status payment payloads of a provider.
func paymentPayloads(statuses []StatusCase, build func(raw string) []byte) map[string][]byte {
	payloads := make(map[string][]byte, len(statuses))
	for _, statusCase := range statuses {
		payloads[statusCase.Raw] = build(statusCase.Raw)
	}
	return payloads
}

func nowPaymentsFixtures() ProfileFixtures {
	const gatewayID = "np-gw-1"
	secret := []byte("np-secret")
	// Relative expiry keeps the fixture usable once the NOWPayments adapter
	// consumes it (the same wall-clock concern as btcpay/bitpay).
	expiration := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	statuses := []StatusCase{
		{Raw: "waiting", Expected: domain.StateDetected},
		{Raw: "confirming", Expected: domain.StateDetected},
		{Raw: "confirmed", Expected: domain.StateDetected},
		{Raw: "sending", Expected: domain.StateDetected},
		{Raw: "partially_paid", Expected: domain.StateUnderpaid},
		{Raw: "finished", Expected: domain.StateConfirmed},
		{Raw: "failed", Expected: domain.StateFailed},
		{Raw: "refunded", Expected: domain.StateFailed},
		{Raw: "expired", Expected: domain.StateExpired},
	}
	payment := func(raw string) []byte {
		return []byte(fmt.Sprintf(`{"payment_id":%q,"order_id":"order-1","payment_status":%q,"price_amount":12.34,"actually_paid":12.34,"pay_currency":"usdttrc20","updated_at":"2026-10-17T12:00:00Z"}`, gatewayID, raw))
	}
	valid := payment("finished")
	return ProfileFixtures{
		Provider:         "nowpayments",
		CreatePath:       "POST /v1/invoice",
		CreateStatus:     201,
		CreateTemplate:   []byte(fmt.Sprintf(`{"id":"np-gw-1","order_id":{{request.order_id}},"price_amount":12.34,"price_currency":"USDT","pay_currency":"usdttrc20","invoice_url":"https://mock-provider.local/nowpayments/checkout/np-gw-1","created_at":"2026-10-17T12:00:00Z","expiration_estimate_date":%q}`, expiration)),
		CreateGatewayID:  gatewayID,
		OrderField:       "order_id",
		CheckNetwork:     "usdttrc20",
		PaymentPath:      "GET /v1/payment/{gateway_id}",
		PaymentGatewayID: gatewayID,
		PaymentByStatus:  paymentPayloads(statuses, payment),
		PaymentUnknown:   payment("totally_unknown"),
		LookupPath:       "GET /v1/payment/?order_id={order}",
		PayoutPath:       "POST /v1/payout",
		PayoutTemplate:   []byte(`{"id":"np-payout-1","status":"waiting"}`),
		PayoutGatewayID:  "np-payout-1",
		Secret:           secret,
		WebhookValid:     valid,
		WebhookHeaders:   signHeaders("nowpayments", secret, valid),
		WebhookTampered:  valid,
		TamperedHeaders:  tamperedHeaders(signHeaders("nowpayments", secret, valid)),
		WebhookMissing:   valid,
		ExpectedEvent:    ExpectedEvent{GatewayID: gatewayID, RawStatus: "finished", Type: domain.StateConfirmed},
		Statuses:         statuses,
	}
}

func coinGateFixtures() ProfileFixtures {
	const gatewayID = "1042"
	secret := []byte("cg-token")
	statuses := []StatusCase{
		{Raw: "new", Expected: domain.StateDetected},
		{Raw: "pending", Expected: domain.StateDetected},
		{Raw: "confirming", Expected: domain.StateDetected},
		{Raw: "paid", Expected: domain.StateConfirmed},
		{Raw: "invalid", Expected: domain.StateFailed},
		{Raw: "expired", Expected: domain.StateExpired},
		{Raw: "canceled", Expected: domain.StateFailed},
		{Raw: "refunded", Expected: domain.StateFailed},
	}
	payment := func(raw string) []byte {
		return []byte(fmt.Sprintf(`{"id":1042,"status":%q,"order_id":"order-1","price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"usdt_trc20","payment_address":"MOCKADDR","created_at":"2026-10-17T12:00:00Z"}`, raw))
	}
	valid := []byte(fmt.Sprintf(`{"id":1042,"order_id":"order-1","status":"paid","price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"usdt_trc20","token":%q,"created_at":"2026-10-17T12:00:00Z"}`, string(secret)))
	tampered := []byte(`{"id":1042,"order_id":"order-1","status":"paid","price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"usdt_trc20","token":"wrong-token","created_at":"2026-10-17T12:00:00Z"}`)
	missing := []byte(`{"id":1042,"order_id":"order-1","status":"paid","price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"usdt_trc20","created_at":"2026-10-17T12:00:00Z"}`)
	return ProfileFixtures{
		Provider:         "coingate",
		CreatePath:       "POST /v2/orders",
		CreateStatus:     201,
		CreateTemplate:   []byte(`{"id":1042,"status":"new","order_id":{{request.order_id}},"price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"usdt_trc20","payment_address":"MOCKADDR","created_at":"2026-10-17T12:00:00Z"}`),
		CreateGatewayID:  gatewayID,
		OrderField:       "order_id",
		PaymentPath:      "GET /v2/orders/{gateway_id}",
		PaymentGatewayID: gatewayID,
		PaymentByStatus:  paymentPayloads(statuses, payment),
		PaymentUnknown:   payment("totally_unknown"),
		LookupPath:       "GET /v2/orders?order_id={order}",
		PayoutPath:       "POST /v2/send_request",
		PayoutTemplate:   []byte(`{"id":777,"state":"new"}`),
		PayoutGatewayID:  "777",
		Secret:           secret,
		WebhookValid:     valid,
		WebhookHeaders:   map[string]string{},
		WebhookTampered:  tampered,
		TamperedHeaders:  map[string]string{},
		WebhookMissing:   missing,
		ExpectedEvent:    ExpectedEvent{GatewayID: gatewayID, RawStatus: "paid", Type: domain.StateConfirmed},
		Statuses:         statuses,
	}
}

func bitPayFixtures() ProfileFixtures {
	const gatewayID = "bp-gw-1"
	secret := []byte("bp-secret")
	// Relative times keep the fixtures usable by wall-clock-sensitive checks.
	invoiceTime := time.Now().UnixMilli()
	expirationTime := time.Now().Add(2 * time.Hour).UnixMilli()
	statuses := []StatusCase{
		{Raw: "new", Expected: domain.StateDetected},
		{Raw: "paid", Expected: domain.StateDetected},
		{Raw: "confirmed", Expected: domain.StateDetected},
		{Raw: "complete", Expected: domain.StateConfirmed},
		{Raw: "expired", Expected: domain.StateExpired},
		{Raw: "invalid", Expected: domain.StateFailed},
	}
	payment := func(raw string) []byte {
		return []byte(fmt.Sprintf(`{"id":%q,"url":"https://mock-provider.local/bitpay/checkout/bp-gw-1","status":%q,"price":12340000,"currency":"USDT","orderId":"order-1","amountPaid":12340000,"confirmations":2,"invoiceTime":%d,"expirationTime":%d,"bitcoinAddress":"MOCKADDR"}`, gatewayID, raw, invoiceTime, expirationTime))
	}
	valid := []byte(`{"event":{"code":1003,"name":"invoice_completed"},"data":{"id":"bp-gw-1","orderId":"order-1","status":"complete","price":12340000,"currency":"USDT","amountPaid":12340000,"confirmations":2,"invoiceTime":1792248000000}}`)
	return ProfileFixtures{
		Provider:         "bitpay",
		CreatePath:       "POST /invoices",
		CreateStatus:     201,
		CreateTemplate:   []byte(fmt.Sprintf(`{"id":"bp-gw-1","url":"https://mock-provider.local/bitpay/checkout/bp-gw-1","status":"new","price":12340000,"currency":"USDT","orderId":{{request.orderId}},"amountPaid":0,"confirmations":0,"invoiceTime":%d,"expirationTime":%d,"bitcoinAddress":"MOCKADDR"}`, invoiceTime, expirationTime)),
		CreateGatewayID:  gatewayID,
		OrderField:       "orderId",
		PaymentPath:      "GET /invoices/{gateway_id}",
		PaymentGatewayID: gatewayID,
		PaymentByStatus:  paymentPayloads(statuses, payment),
		PaymentUnknown:   payment("totally_unknown"),
		LookupPath:       "GET /invoices?orderId={order}",
		PayoutPath:       "POST /payouts",
		PayoutTemplate:   []byte(`{"id":"bp-payout-1","status":"new"}`),
		PayoutGatewayID:  "bp-payout-1",
		Secret:           secret,
		WebhookValid:     valid,
		WebhookHeaders:   signHeaders("bitpay", secret, valid),
		WebhookTampered:  valid,
		TamperedHeaders:  tamperedHeaders(signHeaders("bitpay", secret, valid)),
		WebhookMissing:   valid,
		ExpectedEvent:    ExpectedEvent{GatewayID: gatewayID, RawStatus: "complete", Type: domain.StateConfirmed},
		Statuses:         statuses,
	}
}

func btcPayFixtures() ProfileFixtures {
	const gatewayID = "0123456789abcdef0123456789abcdef"
	secret := []byte("bt-secret")
	// Relative times keep the fixtures usable by wall-clock-sensitive checks
	// (CONF-3 horizon): a static stamp would make the suite expire.
	created := time.Now().Unix()
	expiration := time.Now().Add(2 * time.Hour).Unix()
	statuses := []StatusCase{
		{Raw: "New", Expected: domain.StateDetected},
		{Raw: "Processing", Expected: domain.StateDetected},
		{Raw: "Settled", Expected: domain.StateConfirmed},
		{Raw: "Expired", Expected: domain.StateExpired},
		{Raw: "Invalid", Expected: domain.StateFailed},
	}
	payment := func(raw string) []byte {
		return []byte(fmt.Sprintf(`{"id":%q,"storeId":"mock-store","amount":"12.340000","currency":"USDT","status":%q,"checkoutLink":"https://mock-provider.local/btcpay/checkout/0123456789abcdef0123456789abcdef","createdTime":%d,"expirationTime":%d,"metadata":{"orderId":"order-1","orderNetwork":"trc20"}}`, gatewayID, raw, created, expiration))
	}
	valid := []byte(`{"deliveryId":"01a138fd-0000-7000-8000-0000000000d1","webhookId":"01a138fd-0000-7000-8000-0000000000d2","type":"InvoiceSettled","invoiceId":"0123456789abcdef0123456789abcdef","storeId":"mock-store","metadata":{"orderId":"order-1"}}`)
	return ProfileFixtures{
		Provider:                 "btcpay",
		CreatePath:               "POST /api/v1/stores/mock-store/invoices",
		CreateStatus:             201,
		CreateTemplate:           []byte(fmt.Sprintf(`{"id":"0123456789abcdef0123456789abcdef","storeId":"mock-store","amount":"12.340000","currency":"USDT","status":"New","checkoutLink":"https://mock-provider.local/btcpay/checkout/0123456789abcdef0123456789abcdef","createdTime":%d,"expirationTime":%d,"metadata":{"orderId":{{request.metadata.orderId}},"orderNetwork":"trc20"}}`, created, expiration)),
		CreateGatewayID:          gatewayID,
		OrderField:               "metadata.orderId",
		PaymentPath:              "GET /api/v1/stores/mock-store/invoices/{gateway_id}",
		PaymentGatewayID:         gatewayID,
		PaymentByStatus:          paymentPayloads(statuses, payment),
		PaymentUnknown:           payment("totally_unknown"),
		LookupPath:               "GET /api/v1/stores/mock-store/invoices?orderId={order}",
		PayoutPath:               "POST /api/v1/stores/mock-store/pull-payments",
		PayoutTemplate:           []byte(`{"id":"bt-pp-1","name":"refund"}`),
		PayoutGatewayID:          "bt-pp-1",
		PayoutExecutionPath:      "POST /api/v1/pull-payments/{pull_payment_id}/payouts",
		PayoutExecutionTemplate:  []byte(`{"id":"bt-payout-1","state":"AwaitingApproval","pullPaymentId":"bt-pp-1"}`),
		PayoutExecutionGatewayID: "bt-payout-1",
		Secret:                   secret,
		WebhookValid:             valid,
		WebhookHeaders:           signHeaders("btcpay", secret, valid),
		WebhookTampered:          valid,
		TamperedHeaders:          tamperedHeaders(signHeaders("btcpay", secret, valid)),
		WebhookMissing:           valid,
		ExpectedEvent:            ExpectedEvent{GatewayID: gatewayID, RawStatus: "Settled", Type: domain.StateConfirmed},
		Statuses:                 statuses,
	}
}

func mockFixtures() ProfileFixtures {
	const gatewayID = "mock-gw-1"
	statuses := []StatusCase{
		{Raw: "pending", Expected: domain.StateDetected},
		{Raw: "detected", Expected: domain.StateDetected},
		{Raw: "confirmed", Expected: domain.StateConfirmed},
		{Raw: "failed", Expected: domain.StateFailed},
		{Raw: "expired", Expected: domain.StateExpired},
		{Raw: "underpaid", Expected: domain.StateUnderpaid},
		{Raw: "overpaid", Expected: domain.StateOverpaid},
	}
	payment := func(raw string) []byte {
		return []byte(fmt.Sprintf(`{"gateway_id":%q,"payment_id":"order-1","address":"MOCKADDR","network":"trc20","amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"status":%q,"received_amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"confirmations":0,"txid":"","expires_at":"2026-10-17T12:15:00Z","created_at":"2026-10-17T12:00:00Z"}`, gatewayID, raw))
	}
	valid := []byte(`{"payment_id":"order-1","gateway_id":"mock-gw-1","status":"confirmed","received_amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"confirmations":1,"txid":"0xtx","network":"trc20","event_ts":"2026-10-17T12:00:00Z"}`)
	return ProfileFixtures{
		Provider:         "mock",
		CreatePath:       "POST /api/v1/invoices",
		CreateStatus:     201,
		CreateTemplate:   []byte(`{"gateway_id":"mock-gw-1","payment_id":{{request.payment_id}},"address":"MOCKADDR","network":"trc20","amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"status":"pending","received_amount":{"minor_units":0,"asset":"USDT","decimals":6},"confirmations":0,"expires_at":"2026-10-17T12:15:00Z","created_at":"2026-10-17T12:00:00Z"}`),
		CreateGatewayID:  gatewayID,
		OrderField:       "payment_id",
		PaymentPath:      "GET /api/v1/invoices/{gateway_id}",
		PaymentGatewayID: gatewayID,
		PaymentByStatus:  paymentPayloads(statuses, payment),
		PaymentUnknown:   payment("totally_unknown"),
		Secret:           []byte("mock-secret"),
		WebhookValid:     valid,
		WebhookHeaders:   map[string]string{},
		WebhookTampered:  valid,
		TamperedHeaders:  map[string]string{},
		WebhookMissing:   valid,
		ExpectedEvent:    ExpectedEvent{GatewayID: gatewayID, RawStatus: "confirmed", Type: domain.StateConfirmed},
		Statuses:         statuses,
	}
}
