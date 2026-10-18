package usecase

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
)

// PayoutStatusStore is the storage port of the status worker.
type PayoutStatusStore interface {
	// ListPending returns submitted payouts not refreshed after the time.
	ListPending(ctx context.Context, limit int, refreshedBefore time.Time) ([]storage.ProviderPayoutRow, error)
	// TouchSubmittedTx refreshes the polling timestamp of a submitted payout.
	TouchSubmittedTx(ctx context.Context, tx *gorm.DB, payoutID string, at time.Time) error
	// ResolveTx moves a submitted payout to a terminal state; false means a race.
	ResolveTx(ctx context.Context, tx *gorm.DB, payoutID, state string, patch storage.ProviderPayoutPatch, at time.Time) (bool, error)
}

// PayoutStatusWorker polls submitted payouts at the provider and publishes the
// confirmed/failed verdict exactly once (17.11): the CAS resolution and the
// outbox event share one transaction.
type PayoutStatusWorker struct {
	db       *gorm.DB
	gateways map[string]domain.PaymentGateway
	store    PayoutStatusStore
	outbox   OutboxAppender
	interval time.Duration
	batch    int
	log      *zap.Logger
	clock    func() time.Time
}

// PayoutWorkerOption configures the PayoutStatusWorker.
type PayoutWorkerOption func(*PayoutStatusWorker)

// WithPayoutWorkerClock sets the time source; tests use it.
func WithPayoutWorkerClock(clock func() time.Time) PayoutWorkerOption {
	return func(w *PayoutStatusWorker) {
		if clock != nil {
			w.clock = clock
		}
	}
}

// NewPayoutStatusWorker creates a PayoutStatusWorker.
func NewPayoutStatusWorker(db *gorm.DB, gateways map[string]domain.PaymentGateway, store PayoutStatusStore, appender OutboxAppender, interval time.Duration, batch int, log *zap.Logger, opts ...PayoutWorkerOption) *PayoutStatusWorker {
	if log == nil {
		log = zap.NewNop()
	}
	if interval < time.Second {
		interval = time.Second
	}
	if batch <= 0 {
		batch = 100
	}
	worker := &PayoutStatusWorker{
		db:       db,
		gateways: gateways,
		store:    store,
		outbox:   appender,
		interval: interval,
		batch:    batch,
		log:      log,
		clock:    time.Now,
	}
	for _, opt := range opts {
		opt(worker)
	}
	return worker
}

// Run sweeps immediately and then on the configured interval until cancellation.
func (w *PayoutStatusWorker) Run(ctx context.Context) error {
	sweep := func() {
		if _, err := w.Sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("payout status sweep failed", zap.Error(err))
		}
	}
	sweep()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			sweep()
		}
	}
}

// Sweep polls one batch of pending payouts and returns the number of resolved
// ones. A still-submitted or unreadable payout is only touched: the next poll
// happens after the interval.
func (w *PayoutStatusWorker) Sweep(ctx context.Context) (int, error) {
	now := w.clock().UTC()
	rows, err := w.store.ListPending(ctx, w.batch, now.Add(-w.interval))
	if err != nil {
		return 0, err
	}
	resolved := 0
	for i := range rows {
		row := rows[i]
		if row.GatewayPayoutID == nil || strings.TrimSpace(*row.GatewayPayoutID) == "" {
			// A parked row is touched so it cannot pin the batch head forever.
			w.touch(ctx, row.PayoutID, now)
			continue
		}
		gateway, ok := w.gateways[row.Provider]
		if !ok {
			w.log.Warn("payout provider is not configured",
				zap.String("payout_id", row.PayoutID),
				zap.String("provider", row.Provider))
			w.touch(ctx, row.PayoutID, now)
			continue
		}
		status, err := gateway.GetPayout(ctx, *row.GatewayPayoutID)
		if err != nil {
			if errors.Is(err, domain.ErrGatewayNotFound) {
				if w.resolve(ctx, row, "failed", storage.ProviderPayoutPatch{Reason: stringPtr("gateway payout not found")}) {
					resolved++
				}
				continue
			}
			w.log.Warn("payout status read failed",
				zap.String("payout_id", row.PayoutID),
				zap.String("provider", row.Provider),
				zap.Error(err))
			w.touch(ctx, row.PayoutID, now)
			continue
		}
		switch status.State {
		case domain.PayoutConfirmed:
			patch := storage.ProviderPayoutPatch{}
			if txid := sanitizeIdentifier(status.TXID, 128); txid != "" {
				patch.TXID = &txid
			} else if strings.TrimSpace(status.TXID) != "" {
				w.log.Warn("payout txid is not usable",
					zap.String("payout_id", row.PayoutID),
					zap.String("provider", row.Provider))
			}
			if fee := payoutFee(status.Fee); fee != nil {
				asset := strings.ToUpper(strings.TrimSpace(fee.GetAsset()))
				if !payoutAssetPattern.MatchString(asset) || fee.GetDecimals() > 18 {
					// A bad fee must not wedge the resolution on a CHECK
					// violation: the verdict is recorded without the fee.
					w.log.Warn("payout fee is not usable",
						zap.String("payout_id", row.PayoutID),
						zap.String("provider", row.Provider))
				} else {
					minor := fee.GetMinorUnits()
					decimals := int(fee.GetDecimals())
					patch.FeeMinor = &minor
					patch.FeeAsset = &asset
					patch.FeeDecimals = &decimals
				}
			}
			if w.resolve(ctx, row, "confirmed", patch) {
				resolved++
			}
		case domain.PayoutFailed:
			reason := sanitizeReason(status.Reason)
			if reason == "" {
				reason = "provider reported a payout failure"
			}
			if w.resolve(ctx, row, "failed", storage.ProviderPayoutPatch{Reason: &reason}) {
				resolved++
			}
		default:
			w.touch(ctx, row.PayoutID, now)
		}
	}
	return resolved, nil
}

// resolve applies the terminal state and the event atomically; only the CAS
// winner publishes.
func (w *PayoutStatusWorker) resolve(ctx context.Context, row storage.ProviderPayoutRow, state string, patch storage.ProviderPayoutPatch) bool {
	now := w.clock().UTC()
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        row.PayoutID,
		GatewayPayoutId: derefString(row.GatewayPayoutID),
		Provider:        row.Provider,
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}
	eventType := "gateway.payout.confirmed"
	if state == "failed" {
		event.Type = eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED
		eventType = "gateway.payout.failed"
	}
	if patch.TXID != nil {
		event.Txid = *patch.TXID
	}
	if patch.Reason != nil {
		event.Reason = *patch.Reason
	}
	if patch.FeeMinor != nil && patch.FeeAsset != nil && patch.FeeDecimals != nil {
		event.Fee = &commonv1.Money{MinorUnits: *patch.FeeMinor, Asset: *patch.FeeAsset, Decimals: uint32(*patch.FeeDecimals)} //nolint:gosec // CHECK 0..18
	}
	applied := false
	apply := func(tx *gorm.DB) error {
		won, resolveErr := w.store.ResolveTx(ctx, tx, row.PayoutID, state, patch, now)
		if resolveErr != nil {
			return resolveErr
		}
		if !won {
			return nil
		}
		applied = true
		envelope, envErr := buildPayoutEnvelope(eventType, event, nil, now)
		if envErr != nil {
			return envErr
		}
		return w.outbox.Append(ctx, tx, envelope)
	}
	var err error
	if w.db != nil {
		err = w.db.WithContext(ctx).Transaction(apply)
	} else {
		err = apply(nil)
	}
	if err != nil {
		w.log.Warn("payout resolution failed",
			zap.String("payout_id", row.PayoutID),
			zap.String("state", state),
			zap.Error(err))
		return false
	}
	if !applied {
		return false
	}
	w.log.Info("payout resolved from provider",
		zap.String("payout_id", row.PayoutID),
		zap.String("provider", row.Provider),
		zap.String("state", state))
	return true
}

// touch refreshes the polling timestamp; a touch failure is only logged: the
// next sweep retries the row anyway.
func (w *PayoutStatusWorker) touch(ctx context.Context, payoutID string, at time.Time) {
	if err := w.store.TouchSubmittedTx(ctx, nil, payoutID, at); err != nil {
		w.log.Warn("payout touch failed", zap.String("payout_id", payoutID), zap.Error(err))
	}
}

// stringPtr returns a pointer to the value (test and patch helper).
func stringPtr(value string) *string { return &value }

// derefString returns the value of an optional string.
func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
