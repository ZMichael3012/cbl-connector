package wire_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/providers/wire"
)

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
