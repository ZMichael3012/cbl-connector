package btcpay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
)

// webhookTypes maps Greenfield webhook types to canonical states and the raw
// status keyword the type implies.
var webhookTypes = map[string]struct {
	state domain.State
	raw   string
}{
	"InvoiceCreated":         {state: domain.StateDetected, raw: "New"},
	"InvoiceReceivedPayment": {state: domain.StateDetected, raw: "Processing"},
	"InvoiceProcessing":      {state: domain.StateDetected, raw: "Processing"},
	"InvoiceSettled":         {state: domain.StateConfirmed, raw: "Settled"},
	"InvoiceExpired":         {state: domain.StateExpired, raw: "Expired"},
	"InvoiceInvalid":         {state: domain.StateFailed, raw: "Invalid"},
}

// webhookBody describes the Greenfield webhook delivery body.
type webhookBody struct {
	DeliveryID string            `json:"deliveryId"`
	WebhookID  string            `json:"webhookId"`
	Type       string            `json:"type"`
	InvoiceID  string            `json:"invoiceId"`
	StoreID    string            `json:"storeId"`
	Metadata   map[string]string `json:"metadata"`
}

// btcpaySignature is the manifest-declared verification strategy of the
// provider: raw HMAC-SHA256 hex over the body with the sha256= prefix.
var btcpaySignature = webhooksig.RawHMAC{
	Header:    "BTCPay-Sig",
	Algorithm: webhooksig.SHA256,
	Encoding:  webhooksig.Hex,
	Prefix:    "sha256=",
}

// VerifyWebhook verifies the BTCPay-Sig header over the raw body and parses
// the event; the webhook carries no event time, so the receipt time is used.
func (a *Adapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	header := http.Header{}
	for key, values := range headers {
		for _, value := range values {
			header.Add(key, value)
		}
	}
	if err := btcpaySignature.Verify(a.webhookSecret, webhooksig.Request{Header: header, Body: rawBody}); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrUnsignedWebhook, err)
	}
	var decoded webhookBody
	if err := decodeWebhook(rawBody, &decoded); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrInvalidWebhook, err)
	}
	if decoded.InvoiceID == "" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: webhook has no invoice id", domain.ErrInvalidWebhook)
	}
	if decoded.StoreID != "" && decoded.StoreID != a.storeID {
		// Multi-store validation is out of scope (single store per adapter),
		// but a delivery of another store must not be attributed here.
		return domain.CanonicalEvent{}, fmt.Errorf("%w: webhook belongs to store %q", domain.ErrInvalidWebhook, decoded.StoreID)
	}
	mapped, ok := webhookTypes[decoded.Type]
	if !ok {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: webhook type %q", domain.ErrUnknownState, decoded.Type)
	}
	return domain.CanonicalEvent{
		Type:       mapped.state,
		PaymentID:  decoded.Metadata["orderId"],
		GatewayID:  decoded.InvoiceID,
		Provider:   "btcpay",
		RawStatus:  mapped.raw,
		OccurredAt: time.Now().UTC(),
	}, nil
}

// decodeWebhook parses the delivery body rejecting trailing data.
func decodeWebhook(body []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing json")
	}
	return nil
}
