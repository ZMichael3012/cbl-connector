// Package conformance is the adapter contract testkit of the connector (task
// CBL-17.5, the generalized 3.7): it drives any SPI v1 adapter through the same
// checks against provider wire fixtures, so every current and future adapter is
// proven against one contract. The suite is mandatory for every adapter
// (plan decision 21, checklist 14.4).
package conformance

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
)

// StrategyFromManifest builds the kit verification strategy declared by the
// provider manifest; the suite is its first consumer, and the defense-in-depth
// re-check (T-CONNECTOR-01) reuses it when the connector holds the secret.
func StrategyFromManifest(m domain.ProviderManifest) (webhooksig.Strategy, error) {
	switch m.VerifyStrategy {
	case domain.VerifyStrategyRawHMAC:
		encoding := webhooksig.Hex
		if m.VerifyEncoding == "base64" {
			encoding = webhooksig.Base64
		}
		return webhooksig.RawHMAC{
			Header:    m.VerifyHeader,
			Algorithm: webhooksig.SHA256,
			Encoding:  encoding,
			Prefix:    m.VerifyPrefix,
		}, nil
	case domain.VerifyStrategyCanonicalJSONHMAC:
		return webhooksig.CanonicalJSONHMAC{
			Header:    m.VerifyHeader,
			Algorithm: webhooksig.SHA512,
			Encoding:  webhooksig.Hex,
		}, nil
	case domain.VerifyStrategyTokenEcho:
		return webhooksig.TokenEcho{Field: m.VerifyField}, nil
	default:
		return nil, fmt.Errorf("conformance: provider %q has no verifiable strategy %q", m.ID, m.VerifyStrategy)
	}
}

// SignBody signs a webhook body the way the provider does and returns the
// signature header and value; token echo bodies embed the token and therefore
// have no signable form.
func SignBody(m domain.ProviderManifest, secret, body []byte) (string, string, error) {
	if len(secret) == 0 {
		return "", "", fmt.Errorf("conformance: provider %q has an empty secret", m.ID)
	}
	switch m.VerifyStrategy {
	case domain.VerifyStrategyRawHMAC:
		if m.VerifyHeader == "" {
			return "", "", fmt.Errorf("conformance: provider %q: raw_hmac requires verify_header", m.ID)
		}
		mac := hmac.New(sha256.New, secret)
		mac.Write(body)
		digest := hex.EncodeToString(mac.Sum(nil))
		if m.VerifyEncoding == "base64" {
			digest = base64.StdEncoding.EncodeToString(mac.Sum(nil))
		}
		return m.VerifyHeader, m.VerifyPrefix + digest, nil
	case domain.VerifyStrategyCanonicalJSONHMAC:
		canonical, err := webhooksig.CanonicalJSON(body)
		if err != nil {
			return "", "", fmt.Errorf("conformance: canonicalize body of provider %q: %w", m.ID, err)
		}
		mac := hmac.New(sha512.New, secret)
		mac.Write(canonical)
		return m.VerifyHeader, hex.EncodeToString(mac.Sum(nil)), nil
	default:
		return "", "", fmt.Errorf("conformance: provider %q: strategy %q is not signable", m.ID, m.VerifyStrategy)
	}
}
