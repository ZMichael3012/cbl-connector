package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
)

func TestExportCursorRoundTrip(t *testing.T) {
	t.Parallel()
	for _, offset := range []int{0, 100, 12345} {
		cursor := domain.EncodeExportCursor(offset)
		require.NotEmpty(t, cursor)
		decoded, err := domain.DecodeExportCursor(cursor)
		require.NoError(t, err)
		assert.Equal(t, offset, decoded)
	}
}

func TestExportCursorDefaultsAndRejects(t *testing.T) {
	t.Parallel()
	offset, err := domain.DecodeExportCursor("")
	require.NoError(t, err)
	assert.Zero(t, offset)

	for _, cursor := range []string{"%%%", "bm90LWEtY3Vyc29y", "x1:-1", "x9:5", "eDE6"} {
		_, err := domain.DecodeExportCursor(cursor)
		require.Error(t, err, "cursor %q", cursor)
		assert.ErrorIs(t, err, domain.ErrInvalidGatewayRequest, "cursor %q", cursor)
	}
}

func TestExportPageAndOffsetParams(t *testing.T) {
	t.Parallel()
	offset, page, err := domain.ExportPageParams(domain.EncodeExportCursor(6), 3)
	require.NoError(t, err)
	assert.Equal(t, 6, offset)
	assert.Equal(t, 3, page)

	_, _, err = domain.ExportPageParams("", 0)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest, "a non-positive limit fails closed")

	_, _, err = domain.ExportPageParams(domain.EncodeExportCursor(4), 3)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest, "an unaligned offset fails closed")

	_, _, err = domain.ExportPageParams("%%%", 3)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest, "a malformed cursor fails closed")

	offset, err = domain.ExportOffsetParams(domain.EncodeExportCursor(5), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, offset)

	_, err = domain.ExportOffsetParams("", 0)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
}
