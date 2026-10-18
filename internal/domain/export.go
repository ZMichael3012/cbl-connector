package domain

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ExportRequest describes a per-provider payment export page request (17.13):
// the window is [From, To), the cursor is opaque to the caller and the limit
// bounds one page.
type ExportRequest struct {
	// From sets the inclusive lower window bound.
	From time.Time
	// To sets the exclusive upper window bound.
	To time.Time
	// Limit sets the page size.
	Limit int
	// Cursor sets the opaque continuation cursor; empty means the first page.
	Cursor string
}

// ExportPage describes one page of provider payment records.
type ExportPage struct {
	// Payments holds the canonical status rows; a row with an empty State and
	// a non-empty RawStatus carries a provider word the adapter cannot map. A
	// zero ReceivedAmount with an empty asset means the amount evidence is
	// missing (an unparsable provider amount or no reported deposit); a
	// genuine zero keeps the asset and the scale, so the reconciler can tell
	// "no evidence" from "zero received".
	Payments []PaymentStatus
	// NextCursor continues the walk; empty means the last page.
	NextCursor string
}

// PaymentExporter is the optional SPI v1 interface for per-provider payment
// exports (17.13): the reconciliation pulls provider operations through it.
type PaymentExporter interface {
	// ExportPayments returns one page of canonical provider records for the
	// window; the cursor continues a previous walk.
	ExportPayments(ctx context.Context, req ExportRequest) (ExportPage, error)
}

// exportCursorPrefix versions the export cursor encoding so a future change
// cannot be misread by an old decoder.
const exportCursorPrefix = "x1:"

// EncodeExportCursor encodes the offset of a provider export walk; the value
// is opaque to consumers.
func EncodeExportCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(exportCursorPrefix + strconv.Itoa(offset)))
}

// DecodeExportCursor decodes an opaque export cursor; an empty cursor means
// offset zero. Malformed values fail closed as ErrInvalidGatewayRequest.
func DecodeExportCursor(cursor string) (int, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return 0, fmt.Errorf("%w: malformed export cursor", ErrInvalidGatewayRequest)
	}
	value := string(decoded)
	if !strings.HasPrefix(value, exportCursorPrefix) {
		return 0, fmt.Errorf("%w: unsupported export cursor version", ErrInvalidGatewayRequest)
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(value, exportCursorPrefix))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: malformed export cursor offset", ErrInvalidGatewayRequest)
	}
	return offset, nil
}

// ExportPageParams derives the offset and the 1-based page number of a
// page-based provider export; an offset that does not align with the limit
// cannot be expressed as a page and fails closed.
func ExportPageParams(cursor string, limit int) (int, int, error) {
	if limit <= 0 {
		return 0, 0, fmt.Errorf("%w: export limit must be positive", ErrInvalidGatewayRequest)
	}
	offset, err := DecodeExportCursor(cursor)
	if err != nil {
		return 0, 0, err
	}
	if offset%limit != 0 {
		return 0, 0, fmt.Errorf("%w: export cursor does not align with the limit", ErrInvalidGatewayRequest)
	}
	return offset, offset/limit + 1, nil
}

// ExportOffsetParams derives the offset of an offset-based provider export.
func ExportOffsetParams(cursor string, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("%w: export limit must be positive", ErrInvalidGatewayRequest)
	}
	return DecodeExportCursor(cursor)
}

// ExportNextCursor builds the continuation cursor of a provider export page:
// a full page continues the walk, a short page terminates it.
func ExportNextCursor(offset, count, limit int) string {
	if limit <= 0 || count < limit {
		return ""
	}
	return EncodeExportCursor(offset + count)
}
