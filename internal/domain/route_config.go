package domain

// RouteConfigSnapshot is the cached view of the routing configuration: the
// enabled rules and the disabled provider names (CBL-12.2).
type RouteConfigSnapshot struct {
	// Rules are the enabled routing rules.
	Rules []RouteRule
	// Disabled lists provider names disabled in the administrative contour.
	Disabled []string
}
