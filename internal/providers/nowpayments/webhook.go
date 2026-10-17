package nowpayments

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ipnSignature is the manifest-declared verification strategy of the provider:
// canonical JSON HMAC-SHA512 hex in the x-nowpayments-sig header.
var ipnSignature = webhooksig.CanonicalJSONHMAC{
	Header:    "x-nowpayments-sig",
	Algorithm: webhooksig.SHA512,
	Encoding:  webhooksig.Hex,
}

// VerifyWebhook verifies the canonical JSON signature over the raw body and
// parses the IPN event; a malformed updated_at falls back to the receipt time.
func (a *Adapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	header := http.Header{}
	for key, values := range headers {
		for _, value := range values {
			header.Add(key, value)
		}
	}
	if err := ipnSignature.Verify(a.ipnSecret, webhooksig.Request{Header: header, Body: rawBody}); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrUnsignedWebhook, err)
	}
	var decoded paymentResponse
	if err := json.Unmarshal(rawBody, &decoded); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrInvalidWebhook, err)
	}
	if decoded.PaymentID == "" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: ipn has no payment id", domain.ErrInvalidWebhook)
	}
	state, ok := paymentStates[decoded.PaymentStatus]
	if !ok {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: ipn status %q", domain.ErrUnknownState, decoded.PaymentStatus)
	}
	occurredAt := time.Now().UTC()
	if decoded.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, decoded.UpdatedAt); err == nil {
			occurredAt = parsed.UTC()
		}
	}
	received := money.Money{}
	if decoded.ActuallyPaid.String() != "" && decoded.PayCurrency != "" {
		asset, scale, err := a.resolvePayAsset(decoded.PayCurrency)
		if err != nil {
			return domain.CanonicalEvent{}, err
		}
		parsed, err := money.ParseJSONNumber(decoded.ActuallyPaid, asset, scale)
		if err != nil {
			return domain.CanonicalEvent{}, fmt.Errorf("nowpayments: parse actually paid: %w", err)
		}
		received = parsed
	}
	return domain.CanonicalEvent{
		Type:           state,
		PaymentID:      decoded.OrderID,
		GatewayID:      decoded.PaymentID,
		Provider:       "nowpayments",
		RawStatus:      decoded.PaymentStatus,
		Network:        strings.ToLower(strings.TrimSpace(decoded.PayCurrency)),
		ReceivedAmount: received,
		OccurredAt:     occurredAt,
	}, nil
}
