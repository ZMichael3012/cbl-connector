package bitpay

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ipnSignature is the manifest-declared verification strategy of the provider:
// a raw HMAC-SHA256 of the body in base64 under the x-signature header.
var ipnSignature = webhooksig.RawHMAC{
	Header:    "x-signature",
	Algorithm: webhooksig.SHA256,
	Encoding:  webhooksig.Base64,
}

// webhookBody describes the BitPay IPN body; amounts are decimal major-unit
// numbers of the pricing currency.
type webhookBody struct {
	Event struct {
		Code int    `json:"code"`
		Name string `json:"name"`
	} `json:"event"`
	Data struct {
		ID            string          `json:"id"`
		OrderID       string          `json:"orderId"`
		Status        string          `json:"status"`
		Price         json.RawMessage `json:"price"`
		Currency      string          `json:"currency"`
		AmountPaid    json.RawMessage `json:"amountPaid"`
		Confirmations uint32          `json:"confirmations"`
		InvoiceTime   int64           `json:"invoiceTime"`
	} `json:"data"`
}

// VerifyWebhook verifies the raw body signature and parses the event; an
// absent invoiceTime leaves the event time zero and the normalizer falls back
// to the receipt time (E-5).
func (a *Adapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	header := http.Header{}
	for key, values := range headers {
		for _, value := range values {
			header.Add(key, value)
		}
	}
	if err := ipnSignature.Verify(a.webhookSecret, webhooksig.Request{Header: header, Body: rawBody}); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrUnsignedWebhook, err)
	}
	var decoded webhookBody
	if err := json.Unmarshal(rawBody, &decoded); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrInvalidWebhook, err)
	}
	if decoded.Data.ID == "" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: callback has no invoice id", domain.ErrInvalidWebhook)
	}
	state, ok := invoiceStates[decoded.Data.Status]
	if !ok {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: callback status %q", domain.ErrUnknownState, truncateText([]byte(decoded.Data.Status)))
	}
	received := money.Money{}
	if len(decoded.Data.AmountPaid) > 0 && strings.TrimSpace(decoded.Data.Currency) != "" {
		parsed, err := a.parseAmountLiteral(decoded.Data.AmountPaid, decoded.Data.Currency)
		if err != nil {
			return domain.CanonicalEvent{}, err
		}
		received = parsed
	}
	return domain.CanonicalEvent{
		Type:           state,
		PaymentID:      decoded.Data.OrderID,
		GatewayID:      decoded.Data.ID,
		Provider:       "bitpay",
		RawStatus:      decoded.Data.Status,
		ReceivedAmount: received,
		Confirmations:  decoded.Data.Confirmations,
		OccurredAt:     eventTime(decoded.Data.InvoiceTime),
	}, nil
}
