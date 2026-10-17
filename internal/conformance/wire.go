package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// maxWireBodyBytes bounds the recorded request bodies of the wire mock.
const maxWireBodyBytes = 1 << 20

// RecordedRequest describes one request received by the wire mock.
type RecordedRequest struct {
	// Method sets the HTTP method.
	Method string
	// Path sets the request path.
	Path string
	// Body sets the raw request body.
	Body []byte
}

// Wire is the in-test provider emulator built from fixtures: it serves the
// provider routes, records requests and injects failures. The wire is data
// driven (one generic server for every provider, no provider branches).
type Wire struct {
	testing  *testing.T
	fixtures ProfileFixtures
	server   *httptest.Server

	mu             sync.Mutex
	requests       []RecordedRequest
	payment        []byte
	lookupFound    bool
	failStatus     int
	failRetryAfter int
}

// NewWire starts the wire mock of the fixture set.
func NewWire(t *testing.T, fixtures ProfileFixtures) *Wire {
	t.Helper()
	wire := &Wire{testing: t, fixtures: fixtures}
	wire.server = httptest.NewServer(http.HandlerFunc(wire.handle))
	return wire
}

// URL returns the base URL of the wire mock.
func (w *Wire) URL() string { return w.server.URL }

// Close stops the wire mock.
func (w *Wire) Close() { w.server.Close() }

// Requests returns a copy of the recorded requests in order.
func (w *Wire) Requests() []RecordedRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]RecordedRequest(nil), w.requests...)
}

// SetPaymentStatus selects the payment payload of a documented raw status.
func (w *Wire) SetPaymentStatus(raw string) error {
	payload, ok := w.fixtures.PaymentByStatus[raw]
	if !ok {
		return &missingFixtureError{provider: w.fixtures.Provider, what: "payment status " + raw}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.payment = payload
	return nil
}

// SetPaymentUnknown makes the payment route answer the undocumented status.
func (w *Wire) SetPaymentUnknown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.payment = w.fixtures.PaymentUnknown
}

// SetLookupFound toggles the lookup route between found and missing.
func (w *Wire) SetLookupFound(found bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lookupFound = found
}

// FailNext makes the next request answer with the given status; the Retry-After
// header is set for 429.
func (w *Wire) FailNext(status, retryAfterS int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failStatus = status
	w.failRetryAfter = retryAfterS
}

// missingFixtureError reports a fixture gap (a testkit bug, not an adapter bug).
type missingFixtureError struct {
	provider string
	what     string
}

// Error implements the error interface.
func (e *missingFixtureError) Error() string {
	return "conformance: provider " + e.provider + " fixtures miss " + e.what
}

// handle serves one wire request: failure injection first, then route matching.
func (w *Wire) handle(rw http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWireBodyBytes))
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	w.mu.Lock()
	w.requests = append(w.requests, RecordedRequest{Method: r.Method, Path: r.URL.Path, Body: body})
	failStatus := w.failStatus
	failRetryAfter := w.failRetryAfter
	w.failStatus = 0
	w.failRetryAfter = 0
	payment := w.payment
	lookupFound := w.lookupFound
	w.mu.Unlock()

	rw.Header().Set("Content-Type", "application/json")
	if failStatus != 0 {
		if failStatus == http.StatusTooManyRequests && failRetryAfter > 0 {
			rw.Header().Set("Retry-After", strconv.Itoa(failRetryAfter))
		}
		rw.WriteHeader(failStatus)
		_, _ = rw.Write([]byte(`{"error":"injected failure"}`))
		return
	}

	switch {
	case matchesRoute(w.fixtures.PayoutExecutionPath, r.Method, r.URL.Path):
		rw.WriteHeader(http.StatusCreated)
		_, _ = rw.Write(w.fixtures.PayoutExecutionTemplate)
	case matchesRoute(w.fixtures.PayoutPath, r.Method, r.URL.Path):
		rw.WriteHeader(http.StatusCreated)
		_, _ = rw.Write(w.fixtures.PayoutTemplate)
	case matchesLookup(w.fixtures.LookupPath, r.Method, r.URL):
		if !lookupFound {
			rw.WriteHeader(http.StatusNotFound)
			_, _ = rw.Write([]byte(`{"error":"not found"}`))
			return
		}
		writePayment(rw, payment)
	case matchesRoute(w.fixtures.CreatePath, r.Method, r.URL.Path):
		rendered, err := renderTemplate(w.fixtures.CreateTemplate, body)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			_, _ = rw.Write([]byte(`{"error":"template placeholder is unresolved"}`))
			return
		}
		status := w.fixtures.CreateStatus
		if status == 0 {
			status = http.StatusCreated
		}
		rw.WriteHeader(status)
		_, _ = rw.Write(rendered)
	case matchesRoute(w.fixtures.PaymentPath, r.Method, r.URL.Path):
		if payment == nil {
			rw.WriteHeader(http.StatusNotFound)
			_, _ = rw.Write([]byte(`{"error":"payment fixture is not selected"}`))
			return
		}
		writePayment(rw, payment)
	default:
		rw.WriteHeader(http.StatusNotFound)
		_, _ = rw.Write([]byte(`{"error":"unknown route"}`))
	}
}

// writePayment answers the payment payload.
func writePayment(rw http.ResponseWriter, payment []byte) {
	if payment == nil {
		rw.WriteHeader(http.StatusNotFound)
		_, _ = rw.Write([]byte(`{"error":"payment fixture is not selected"}`))
		return
	}
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write(payment)
}

// matchesRoute checks a "METHOD /path" fixture pattern against a request; a
// "{name}" segment matches any single path segment.
func matchesRoute(fixture, method, path string) bool {
	if fixture == "" {
		return false
	}
	fixtureMethod, fixturePath := splitFixture(fixture)
	if fixtureMethod != method {
		return false
	}
	return matchesPattern(fixturePath, path)
}

// matchesLookup checks the lookup route: the pattern carries a query string and
// the request must carry one too.
func matchesLookup(fixture, method string, requestURL *url.URL) bool {
	if fixture == "" {
		return false
	}
	fixtureMethod, fixturePath := splitFixture(fixture)
	if fixtureMethod != method {
		return false
	}
	pathPattern := fixturePath
	if idx := strings.IndexByte(fixturePath, '?'); idx >= 0 {
		pathPattern = fixturePath[:idx]
		if requestURL.RawQuery == "" {
			return false
		}
	} else if requestURL.RawQuery != "" {
		return false
	}
	return matchesPattern(pathPattern, requestURL.Path)
}

// splitFixture splits a "METHOD /path" fixture into its parts.
func splitFixture(fixture string) (string, string) {
	if space := strings.IndexByte(fixture, ' '); space >= 0 {
		return fixture[:space], fixture[space+1:]
	}
	return "", fixture
}

// matchesPattern matches a path pattern with "{name}" wildcards.
func matchesPattern(pattern, path string) bool {
	patternParts := strings.Split(pattern, "/")
	pathParts := strings.Split(path, "/")
	if len(patternParts) != len(pathParts) {
		return false
	}
	for i := range patternParts {
		part := patternParts[i]
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			if pathParts[i] == "" {
				return false
			}
			continue
		}
		if part != pathParts[i] {
			return false
		}
	}
	return true
}

// templatePlaceholder matches {{request.<dotted.path>}} placeholders.
var templatePlaceholder = regexp.MustCompile(`\{\{request\.([a-zA-Z0-9_.]+)\}\}`)

// renderTemplate substitutes the request values of a response template; the
// template places {{request.<path>}} outside of quotes, because the rendered
// value is JSON marshaled (a string value arrives quoted). An unresolved
// placeholder is a fixture bug and fails the request with 400 (spec E-3).
func renderTemplate(template, requestBody []byte) ([]byte, error) {
	var decoded map[string]any
	if err := json.Unmarshal(requestBody, &decoded); err != nil {
		return nil, fmt.Errorf("conformance: decode request body: %w", err)
	}
	unresolved := false
	rendered := templatePlaceholder.ReplaceAllFunc(template, func(match []byte) []byte {
		path := string(templatePlaceholder.FindSubmatch(match)[1])
		value, ok := lookupJSONPath(decoded, path)
		if !ok {
			unresolved = true
			return match
		}
		marshaled, err := json.Marshal(value)
		if err != nil {
			unresolved = true
			return match
		}
		return marshaled
	})
	if unresolved {
		return nil, fmt.Errorf("conformance: template has unresolved request placeholders")
	}
	return rendered, nil
}

// lookupJSONPath resolves a dotted path over a decoded JSON object.
func lookupJSONPath(root map[string]any, path string) (any, bool) {
	var current any = root
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}
