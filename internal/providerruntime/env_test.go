package providerruntime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCapabilitiesFromEnv(t *testing.T) {
	valid := `[{"network":"usdt_trc20","asset":"usdt","min_minor":1000000,"max_minor":50000000,"decimals":6,"confirmations":2}]`
	got, err := capabilitiesFromEnv(valid)
	require.NoError(t, err)
	require.Contains(t, got.Networks, "usdt_trc20")
	row := got.Networks["usdt_trc20"]
	assert.Equal(t, "USDT", row.Asset)
	assert.EqualValues(t, 1000000, row.MinAmount.MinorUnits)
	assert.EqualValues(t, 50000000, row.MaxAmount.MinorUnits)
	assert.Equal(t, uint32(2), row.ConfirmationDepth)

	invalid := map[string]string{
		"unknown field":     `[{"network":"usdt_trc20","asset":"USDT","min_minor":1,"decimals":6,"fee":1}]`,
		"empty":             `[]`,
		"missing network":   `[{"asset":"USDT","min_minor":1,"decimals":6}]`,
		"missing asset":     `[{"network":"usdt_trc20","min_minor":1,"decimals":6}]`,
		"zero decimals":     `[{"network":"usdt_trc20","asset":"USDT","min_minor":1}]`,
		"negative min":      `[{"network":"usdt_trc20","asset":"USDT","min_minor":-1,"decimals":6}]`,
		"negative max":      `[{"network":"usdt_trc20","asset":"USDT","min_minor":1,"max_minor":-1,"decimals":6}]`,
		"inverted bounds":   `[{"network":"usdt_trc20","asset":"USDT","min_minor":10,"max_minor":5,"decimals":6}]`,
		"duplicate network": `[{"network":"usdt_trc20","asset":"USDT","min_minor":1,"decimals":6},{"network":"usdt_trc20","asset":"USDT","min_minor":1,"decimals":6}]`,
		"trailing json":     valid + `{}`,
		"malformed":         `{`,
	}
	for id, raw := range invalid {
		t.Run(id, func(t *testing.T) {
			_, err := capabilitiesFromEnv(raw)
			require.Error(t, err)
		})
	}
}

func TestDurationEnv(t *testing.T) {
	getenv := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	assert.Equal(t, 10*time.Second,
		durationEnv(zap.NewNop(), getenv(map[string]string{}), "KEY", 10*time.Second))
	assert.Equal(t, 10*time.Second,
		durationEnv(zap.NewNop(), getenv(map[string]string{"KEY": "nope"}), "KEY", 10*time.Second))
	assert.Equal(t, 10*time.Second,
		durationEnv(zap.NewNop(), getenv(map[string]string{"KEY": "0s"}), "KEY", 10*time.Second))
	assert.Equal(t, 10*time.Second,
		durationEnv(zap.NewNop(), getenv(map[string]string{"KEY": "-2s"}), "KEY", 10*time.Second))
	assert.Equal(t, 2*time.Second,
		durationEnv(zap.NewNop(), getenv(map[string]string{"KEY": "2s"}), "KEY", 10*time.Second))
}
