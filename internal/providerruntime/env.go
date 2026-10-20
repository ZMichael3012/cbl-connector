package providerruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// capabilityEnvRow describes one network row of the adapter capabilities env.
type capabilityEnvRow struct {
	Network       string `json:"network"`
	Asset         string `json:"asset"`
	MinMinor      int64  `json:"min_minor"`
	MaxMinor      int64  `json:"max_minor"`
	Decimals      uint32 `json:"decimals"`
	Confirmations uint32 `json:"confirmations"`
}

// capabilitiesFromEnv parses the adapter capability rows of the deployment
// (the catalog artifact 17.21 replaces this configuration later); unknown
// fields, duplicate networks, missing decimals and inverted bounds fail fast
// (spec E-7).
func capabilitiesFromEnv(raw string) (domain.Capabilities, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var rows []capabilityEnvRow
	if err := decoder.Decode(&rows); err != nil {
		return domain.Capabilities{}, fmt.Errorf("capabilities env: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.Capabilities{}, errors.New("capabilities env: unexpected trailing json")
	}
	if len(rows) == 0 {
		return domain.Capabilities{}, errors.New("capabilities env: no rows")
	}
	networks := make(map[string]domain.NetworkCapability, len(rows))
	for _, row := range rows {
		network := strings.TrimSpace(row.Network)
		asset := strings.ToUpper(strings.TrimSpace(row.Asset))
		if network == "" || asset == "" {
			return domain.Capabilities{}, errors.New("capabilities env: network and asset are required")
		}
		if row.Decimals == 0 {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: decimals are required for network %q", network)
		}
		if row.MinMinor < 0 || row.MaxMinor < 0 {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: negative bounds for network %q", network)
		}
		if row.MaxMinor != 0 && row.MaxMinor < row.MinMinor {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: max below min for network %q", network)
		}
		if _, duplicate := networks[network]; duplicate {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: duplicate network %q", network)
		}
		minAmount, err := money.New(row.MinMinor, asset, row.Decimals)
		if err != nil {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: min amount: %w", err)
		}
		maximum := money.Money{Asset: asset, Decimals: row.Decimals}
		if row.MaxMinor != 0 {
			maximum, err = money.New(row.MaxMinor, asset, row.Decimals)
			if err != nil {
				return domain.Capabilities{}, fmt.Errorf("capabilities env: max amount: %w", err)
			}
		}
		networks[network] = domain.NetworkCapability{
			Asset:             asset,
			MinAmount:         minAmount,
			MaxAmount:         maximum,
			ConfirmationDepth: row.Confirmations,
		}
	}
	return domain.Capabilities{Networks: networks}, nil
}

// durationEnv reads a duration from an environment variable; an invalid or
// non-positive value falls back to the default with a warning.
func durationEnv(log *zap.Logger, getenv func(string) string, key string, fallback time.Duration) time.Duration {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		log.Warn("invalid duration env, using default",
			zap.String("key", key),
			zap.String("value", raw),
			zap.Duration("default", fallback),
		)
		return fallback
	}
	return parsed
}
