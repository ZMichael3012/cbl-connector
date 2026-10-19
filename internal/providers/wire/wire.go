// Package wire contains provider-agnostic HTTP payload helpers shared by the
// adapters (extracted at the second adapter per decision 21).
package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strings"

	"github.com/cbl/cbl-kit/money"
)

// DecodeArrayOrObject decodes a provider body that may be a JSON array or a
// single object. An array yields its first element, decoded by streaming so a
// hostile long list is not materialized in memory; an empty array reports the
// given not-found class (for example domain.ErrGatewayNotFound); a malformed
// payload is a decode error, never a not-found. The mock stand answers single
// objects where real providers answer arrays (CBL-17.9 D4).
func DecodeArrayOrObject[T any](payload []byte, notFound error) (T, error) {
	var zero T
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		if _, err := decoder.Token(); err != nil { // consume the opening bracket
			return zero, fmt.Errorf("providers/wire: decode list: %w", err)
		}
		if !decoder.More() {
			return zero, notFound
		}
		var first T
		if err := decoder.Decode(&first); err != nil {
			return zero, fmt.Errorf("providers/wire: decode list element: %w", err)
		}
		return first, nil
	}
	var single T
	if err := json.Unmarshal(trimmed, &single); err != nil {
		return zero, fmt.Errorf("providers/wire: decode object: %w", err)
	}
	return single, nil
}

// Redact removes configured secrets (raw and URL-encoded) from an untrusted
// provider excerpt so a hostile or misconfigured endpoint that echoes the
// request cannot leak a token into logs or DLQ texts (security review 17.26).
func Redact(payload []byte, secrets ...string) []byte {
	text := string(payload)
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		text = strings.ReplaceAll(text, secret, "[redacted]")
		if encoded := url.QueryEscape(secret); encoded != secret {
			text = strings.ReplaceAll(text, encoded, "[redacted]")
		}
	}
	return []byte(text)
}

// StablecoinAssets are priced at par in a configured fiat price currency by
// the hosted providers: CoinGate rejects stablecoins as price currencies and
// the NOWPayments sandbox cannot estimate a stablecoin price currency for a
// stablecoin pay currency (both verified live 19.10.2026, T-CONNECTOR-27/28).
var StablecoinAssets = map[string]struct{}{
	"USDT": {}, "USDC": {}, "DAI": {}, "GUSD": {}, "BUSD": {}, "USDP": {},
}

// DefaultPriceCurrency prices stablecoin orders when a provider config omits
// the currency.
const DefaultPriceCurrency = "USD"

// FiatScale is the price scale of hosted-provider fiat prices (cents).
const FiatScale = 2

// ValidatePriceCurrency normalizes and validates a configured fiat price
// currency: 3 ASCII letters, not a stablecoin (the hosted provider prices are
// fiat, T-CONNECTOR-27/28). An empty value means the default.
func ValidatePriceCurrency(raw string) (string, error) {
	code := strings.ToUpper(strings.TrimSpace(raw))
	if code == "" {
		code = DefaultPriceCurrency
	}
	if len(code) != 3 {
		return "", fmt.Errorf("price currency %q must be a 3-letter code", raw)
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return "", fmt.Errorf("price currency %q must be 3 ASCII letters", raw)
		}
	}
	if _, isStable := StablecoinAssets[code]; isStable {
		return "", fmt.Errorf("price currency %q is a stablecoin: hosted provider prices are fiat", code)
	}
	return code, nil
}

// FiatPriceCurrency resolves the fiat price currency of an amount: the
// configured fiat prices itself, a known stablecoin is priced at par in it,
// and anything else is rejected until a real rate is wired (T-CONNECTOR-27).
func FiatPriceCurrency(asset, priceCurrency string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(asset))
	if upper == priceCurrency {
		return upper, nil
	}
	if _, ok := StablecoinAssets[upper]; ok {
		return priceCurrency, nil
	}
	return "", fmt.Errorf("asset %q cannot be priced in %s: a rate is not wired yet", asset, priceCurrency)
}

// FiatPriceAmount renders an amount as a hosted-provider fiat price: the amount
// is converted to the fiat scale (half-up) and denominated in the price
// currency (T-CONNECTOR-27). The conversions are overflow-checked: a wrapped
// price must never reach the provider.
func FiatPriceAmount(amount money.Money, priceCurrency string, assetScale uint32) (string, error) {
	units := amount.MinorUnits
	switch {
	case assetScale > FiatScale:
		divisor := pow10(assetScale - FiatScale)
		if units > math.MaxInt64-divisor/2 {
			return "", fmt.Errorf("amount is too large to price in %s", priceCurrency)
		}
		units = (units + divisor/2) / divisor
	case assetScale < FiatScale:
		factor := pow10(FiatScale - assetScale)
		if units > math.MaxInt64/factor {
			return "", fmt.Errorf("amount is too large to price in %s", priceCurrency)
		}
		units *= factor
	}
	return money.Money{MinorUnits: units, Asset: priceCurrency, Decimals: FiatScale}.String(), nil
}

// pow10 returns 10^n for the scale conversions of FiatPriceAmount.
func pow10(n uint32) int64 {
	value := int64(1)
	for i := uint32(0); i < n; i++ {
		value *= 10
	}
	return value
}

// DecodeEnvelopeOrArray decodes a provider list body that may be a bare JSON
// array or an envelope object holding the rows under the given key (for
// example "data" or "orders"); the reconciliation exports of the adapters
// share it (17.13).
func DecodeEnvelopeOrArray[T any](payload []byte, key string) ([]T, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var rows []T
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return nil, fmt.Errorf("providers/wire: decode list: %w", err)
		}
		return rows, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return nil, fmt.Errorf("providers/wire: decode list envelope: %w", err)
	}
	raw, ok := envelope[key]
	if !ok {
		// A missing envelope key is a shape violation, not an empty page:
		// silently answering "no operations" would hide provider API drift.
		return nil, fmt.Errorf("providers/wire: list envelope has no %q key", key)
	}
	var rows []T
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("providers/wire: decode list rows: %w", err)
	}
	return rows, nil
}
