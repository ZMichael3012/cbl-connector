package domain

import "errors"

// Connector domain errors; the transport maps them to platform classes.
var (
	// ErrUnknownState reports an unknown provider status.
	ErrUnknownState = errors.New("connector: unknown provider state")
	// ErrInvalidRoute reports an invalid routing request.
	ErrInvalidRoute = errors.New("connector: invalid route request")
	// ErrUnsupportedNetwork reports an unsupported network or asset.
	ErrUnsupportedNetwork = errors.New("connector: unsupported network or asset")
	// ErrProviderUnavailable reports that the provider is unavailable.
	ErrProviderUnavailable = errors.New("connector: provider unavailable")
	// ErrUnsignedWebhook reports a webhook without a verified signature.
	ErrUnsignedWebhook = errors.New("connector: webhook signature not verified")
	// ErrUnknownProvider reports a webhook from an unknown provider.
	ErrUnknownProvider = errors.New("connector: unknown provider")
	// ErrInvalidWebhook reports an invalid webhook payload.
	ErrInvalidWebhook = errors.New("connector: invalid webhook payload")
	// ErrTemporary reports a temporary provider failure: the call can be retried.
	ErrTemporary = errors.New("connector: temporary provider failure")
	// ErrRejected reports a provider rejection at the business rule level: a retry is useless.
	ErrRejected = errors.New("connector: provider rejected the request")
	// ErrProviderOverloaded reports exhaustion of the provider bulkhead.
	ErrProviderOverloaded = errors.New("connector: provider overloaded")
	// ErrRefundsUnsupported reports a provider without a refunds API: the
	// refund stays on the manual path (17.12, decision 8).
	ErrRefundsUnsupported = errors.New("connector: provider does not support refunds")
	// ErrInvalidGatewayRequest reports an invalid gateway request (caller bug).
	ErrInvalidGatewayRequest = errors.New("connector: invalid gateway request")
	// ErrInvalidAdminInput reports an invalid administrative input (CBL-12.2).
	ErrInvalidAdminInput = errors.New("connector: invalid admin input")
	// ErrRouteRevisionConflict reports a routing configuration revision mismatch:
	// another operator replaced the configuration since the last read (CBL-12.2).
	ErrRouteRevisionConflict = errors.New("connector: routing revision conflict")
	// ErrProviderNotFound reports a provider unknown to the registry and the rules.
	ErrProviderNotFound = errors.New("connector: provider not found")
	// ErrGatewayNotFound reports a provider payment missing at the provider
	// (the 404 of real provider APIs); the conformance contract CONF-8 and the
	// LookupGateway semantics use it.
	ErrGatewayNotFound = errors.New("connector: gateway payment not found")
)
