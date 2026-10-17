package coingate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
)

// tokenEcho is the manifest-declared verification strategy of the provider: the
// callback body carries the token field compared in constant time.
var tokenEcho = webhooksig.TokenEcho{Field: "token"}

// callbackBody describes the CoinGate callback body.
type callbackBody struct {
	ID            json.Number `json:"id"`
	OrderID       string      `json:"order_id"`
	Status        string      `json:"status"`
	PriceAmount   string      `json:"price_amount"`
	PriceCurrency string      `json:"price_currency"`
	PayAmount     string      `json:"pay_amount"`
	PayCurrency   string      `json:"pay_currency"`
	Token         string      `json:"token"`
	CreatedAt     string      `json:"created_at"`
}

// VerifyWebhook verifies the token echo over the raw body and parses the event;
// the verified callback carries no event time, so the receipt time is used (D4).
func (a *Adapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	header := http.Header{}
	for key, values := range headers {
		for _, value := range values {
			header.Add(key, value)
		}
	}
	if err := tokenEcho.Verify(a.ipnToken, webhooksig.Request{Header: header, Body: rawBody}); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrUnsignedWebhook, err)
	}
	var decoded callbackBody
	if err := json.Unmarshal(rawBody, &decoded); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrInvalidWebhook, err)
	}
	if decoded.ID.String() == "" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: callback has no order id", domain.ErrInvalidWebhook)
	}
	state, ok := orderStates[decoded.Status]
	if !ok {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: callback status %q", domain.ErrUnknownState, decoded.Status)
	}
	return domain.CanonicalEvent{
		Type:       state,
		PaymentID:  decoded.OrderID,
		GatewayID:  decoded.ID.String(),
		Provider:   "coingate",
		RawStatus:  decoded.Status,
		OccurredAt: time.Now().UTC(),
	}, nil
}
