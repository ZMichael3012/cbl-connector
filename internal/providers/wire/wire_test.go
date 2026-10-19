package wire_test

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/providers/wire"
)

func TestFiatPriceHelpers(t *testing.T) {
	t.Parallel()

	currency, err := wire.ValidatePriceCurrency("")
	require.NoError(t, err)
	assert.Equal(t, "USD", currency, "an empty config means the default")
	_, err = wire.ValidatePriceCurrency("USDT")
	require.Error(t, err, "a stablecoin price currency is rejected")
	_, err = wire.ValidatePriceCurrency("1$A")
	require.Error(t, err, "a non-letter code is rejected")

	priced, err := wire.FiatPriceCurrency("USDT", "EUR")
	require.NoError(t, err)
	assert.Equal(t, "EUR", priced, "a stablecoin is priced at par in the configured fiat")
	_, err = wire.FiatPriceCurrency("ETH", "EUR")
	require.Error(t, err, "an asset without a rate is rejected")

	text, err := wire.FiatPriceAmount(money.Money{MinorUnits: 12345000, Asset: "USDT", Decimals: 6}, "USD", 6)
	require.NoError(t, err)
	assert.Equal(t, "12.35", text, "the par price rounds half-up to cents")

	_, err = wire.FiatPriceAmount(money.Money{MinorUnits: math.MaxInt64, Asset: "USDT", Decimals: 6}, "USD", 6)
	require.Error(t, err, "the conversion is overflow-checked")
}

func TestRedact(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"error":"blocked /invoices?token=sec/ret+1 body sec/ret+1"}`)
	redacted := wire.Redact(raw, "sec/ret+1", "")
	assert.Equal(t, `{"error":"blocked /invoices?token=[redacted] body [redacted]"}`, string(redacted))

	encoded := []byte(`{"error":"blocked /invoices?token=sec%2Fret%2B1"}`)
	assert.Equal(t, `{"error":"blocked /invoices?token=[redacted]"}`, string(wire.Redact(encoded, "sec/ret+1")))

	assert.Equal(t, raw, wire.Redact(raw), "no secrets configured leaves the excerpt untouched")
}

type sample struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

var errNotFound = errors.New("not found")

func TestDecodeArrayOrObject(t *testing.T) {
	t.Parallel()

	object, err := wire.DecodeArrayOrObject[sample]([]byte(`{"id":1,"status":"new"}`), errNotFound)
	require.NoError(t, err)
	assert.Equal(t, sample{ID: 1, Status: "new"}, object)

	array, err := wire.DecodeArrayOrObject[sample]([]byte(`[{"id":2,"status":"paid"},{"id":3}]`), errNotFound)
	require.NoError(t, err)
	assert.Equal(t, sample{ID: 2, Status: "paid"}, array)

	_, err = wire.DecodeArrayOrObject[sample]([]byte(`[]`), errNotFound)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNotFound), "an empty array reports the not-found class: %v", err)

	_, err = wire.DecodeArrayOrObject[sample]([]byte(`{`), errNotFound)
	require.Error(t, err)
	assert.False(t, errors.Is(err, errNotFound), "a malformed payload is not a not-found")

	_, err = wire.DecodeArrayOrObject[sample]([]byte(` `), errNotFound)
	require.Error(t, err)
}
