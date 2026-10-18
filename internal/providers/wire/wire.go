// Package wire contains provider-agnostic HTTP payload helpers shared by the
// adapters (extracted at the second adapter per decision 21).
package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
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
