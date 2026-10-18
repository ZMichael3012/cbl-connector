//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// payoutPipelineID is the platform payout of the e2e scenario.
const payoutPipelineID = "01a14f70-0000-7000-8000-000000000501"

// e2ePayoutGateway is a capable provider for the payout pipeline e2e.
type e2ePayoutGateway struct {
	provider    string
	caps        domain.Capabilities
	createCalls int
	refundCalls int
}

func (g *e2ePayoutGateway) CreateInvoice(context.Context, domain.CreateInvoiceRequest) (domain.Invoice, error) {
	return domain.Invoice{}, errors.New("unused")
}

func (g *e2ePayoutGateway) GetPayment(context.Context, string) (domain.PaymentStatus, error) {
	return domain.PaymentStatus{}, errors.New("unused")
}

func (g *e2ePayoutGateway) VerifyWebhook(map[string][]string, []byte) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, errors.New("unused")
}

func (g *e2ePayoutGateway) CreatePayout(context.Context, domain.PayoutRequest) (domain.Payout, error) {
	g.createCalls++
	return domain.Payout{GatewayPayoutID: "po-e2e-1", Provider: g.provider, State: domain.PayoutSubmitted}, nil
}

func (g *e2ePayoutGateway) GetPayout(context.Context, string) (domain.PayoutStatus, error) {
	fee, err := money.New(250, "USDT", 2)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	return domain.PayoutStatus{State: domain.PayoutConfirmed, TXID: "tx-e2e-1", Fee: fee}, nil
}

func (g *e2ePayoutGateway) CreateRefund(context.Context, domain.RefundRequest) (domain.Refund, error) {
	g.refundCalls++
	return domain.Refund{GatewayRefundID: "rf-e2e-1", Provider: g.provider}, nil
}

func (g *e2ePayoutGateway) GetRefund(context.Context, string) (domain.PayoutStatus, error) {
	return domain.PayoutStatus{State: domain.PayoutConfirmed, TXID: "rf-tx-1"}, nil
}

func (g *e2ePayoutGateway) Capabilities() domain.Capabilities { return g.caps }

func createPayoutTopics(t *testing.T, brokers []string) {
	t.Helper()
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topics := []string{
		usecase.TopicPayoutCommands,
		usecase.TopicPayoutCommands + inbox.RetrySuffix5s,
		usecase.TopicPayoutCommands + inbox.RetrySuffix1m,
		usecase.TopicPayoutCommands + inbox.RetrySuffix10m,
		usecase.TopicGatewayPayoutEvents,
	}
	for _, topic := range topics {
		_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
		require.NoError(t, err, topic)
	}
}

// payoutCommandRecord builds a Kafka command record with a fresh event id.
func payoutCommandRecord(t *testing.T) *kgo.Record {
	t.Helper()
	command := &eventsv1.PayoutCommand{
		PayoutId:       payoutPipelineID,
		Amount:         &commonv1.Money{MinorUnits: 100_000, Asset: "USDT", Decimals: 2},
		Network:        "trc20",
		Address:        "TXyzAddress0123456789",
		IdempotencyKey: "payout-e2e-key",
	}
	body, err := proto.Marshal(command)
	require.NoError(t, err)
	return &kgo.Record{
		Topic: usecase.TopicPayoutCommands,
		Key:   []byte(payoutPipelineID),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
			{Key: outbox.HeaderEventType, Value: []byte("payout.command")},
			{Key: outbox.HeaderProducer, Value: []byte("cbl-payout")},
		},
	}
}

// pollMaybe returns the next record or nil when nothing arrives in the timeout.
func pollMaybe(t *testing.T, consumer *kgo.Client, timeout time.Duration) *kgo.Record {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := consumer.PollFetches(pollCtx)
		cancel()
		// A deadline with no records is a normal empty poll here.
		var found *kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			if found == nil {
				found = record
			}
		})
		if found != nil {
			return found
		}
	}
	return nil
}

func TestE2EPayoutPipeline(t *testing.T) {
	db := setupPostgres(t)
	brokers := setupKafka(t)
	createPayoutTopics(t, brokers)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gateway := &e2ePayoutGateway{
		provider: "coingate",
		caps: domain.Capabilities{
			Provider: "coingate",
			Networks: map[string]domain.NetworkCapability{"trc20": {Asset: "USDT"}},
		},
	}
	router, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{"coingate": gateway},
		map[string]bool{"coingate": true},
	)
	require.NoError(t, err)

	repo := storage.NewProviderPayoutsRepository(db)
	invoiceRepo := storage.NewProviderInvoicesRepository(db)
	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithInterval(50*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	group := "cbl-connector-payouts-e2e"
	processor := inbox.NewProcessor(group, db, inbox.NewGuard(),
		usecase.NewPayoutProcessor(router, repo, outbox.NewStore(), zap.NewNop(),
			usecase.WithRefundMapping(invoiceRepo)),
		publisher, inbox.NewRouter(usecase.TopicPayoutCommands, 3), zap.NewNop())
	topics := []string{
		usecase.TopicPayoutCommands,
		usecase.TopicPayoutCommands + inbox.RetrySuffix5s,
		usecase.TopicPayoutCommands + inbox.RetrySuffix1m,
		usecase.TopicPayoutCommands + inbox.RetrySuffix10m,
	}
	runner, err := kafkaconsumer.New(brokers, group, topics, processor, zap.NewNop())
	require.NoError(t, err)
	defer runner.Close()
	go func() { _ = runner.Run(ctx) }()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(usecase.TopicGatewayPayoutEvents),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	require.NoError(t, producer.ProduceSync(ctx, payoutCommandRecord(t)).FirstErr())
	submitted := pollMaybe(t, consumer, 30*time.Second)
	require.NotNil(t, submitted, "the submitted event must reach gateway.payout.events")
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(submitted.Value, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, event.GetType())
	assert.Equal(t, "coingate", event.GetProvider())
	assert.Equal(t, "po-e2e-1", event.GetGatewayPayoutId())
	assert.Equal(t, 1, gateway.createCalls)

	// The status worker resolves the payout exactly once.
	time.Sleep(1100 * time.Millisecond)
	worker := usecase.NewPayoutStatusWorker(db,
		map[string]domain.PaymentGateway{"coingate": gateway},
		repo, outbox.NewStore(), time.Second, 100, zap.NewNop())
	resolved, err := worker.Sweep(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, resolved)

	confirmed := pollMaybe(t, consumer, 30*time.Second)
	require.NotNil(t, confirmed, "the confirmed event must reach gateway.payout.events")
	require.NoError(t, proto.Unmarshal(confirmed.Value, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, event.GetType())
	assert.Equal(t, "tx-e2e-1", event.GetTxid())
	require.NotNil(t, event.GetFee())
	assert.Equal(t, int64(250), event.GetFee().GetMinorUnits())

	// A replay with a new event id is acknowledged without a second provider call.
	require.NoError(t, producer.ProduceSync(ctx, payoutCommandRecord(t)).FirstErr())
	require.Eventually(t, func() bool {
		var count int64
		require.NoError(t, db.Model(&inbox.Record{}).Where("consumer = ?", group).Count(&count).Error)
		return count == 2
	}, 20*time.Second, 200*time.Millisecond, "the replay must reach the inbox")
	assert.Equal(t, 1, gateway.createCalls, "the recorded payout never reaches the provider again")

	var rows int64
	require.NoError(t, db.Table("provider_payouts").Where("payout_id = ?", payoutPipelineID).Count(&rows).Error)
	assert.EqualValues(t, 1, rows)

	// No third event: the replay publishes nothing.
	assert.Nil(t, pollMaybe(t, consumer, 2*time.Second), "the replay must not publish an event")

	// A refund command goes through the provider refunds API of the original
	// provider and is resolved through GetRefund (17.12).
	const refundSourceID = "01a14f70-0000-7000-8000-000000000502"
	const refundPayoutID = "01a14f70-0000-7000-8000-000000000503"
	_, _, err = invoiceRepo.Save(ctx, domain.ProviderInvoice{
		Provider: "coingate", GatewayID: "inv-e2e", PaymentID: refundSourceID,
		OrderID: refundSourceID, RequestHash: "hash-refund",
		CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	refundCommand := &eventsv1.PayoutCommand{
		PayoutId:        refundPayoutID,
		Amount:          &commonv1.Money{MinorUnits: 50_000, Asset: "USDT", Decimals: 2},
		Network:         "trc20",
		Address:         "TXyzAddress0123456789",
		IdempotencyKey:  "payout:" + refundPayoutID + ":command",
		RefundInvoiceId: refundSourceID,
	}
	refundBody, err := proto.Marshal(refundCommand)
	require.NoError(t, err)
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
		Topic: usecase.TopicPayoutCommands,
		Key:   []byte(refundPayoutID),
		Value: refundBody,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
		},
	}).FirstErr())

	refundSubmitted := pollMaybe(t, consumer, 30*time.Second)
	require.NotNil(t, refundSubmitted, "the refund submitted event must reach gateway.payout.events")
	require.NoError(t, proto.Unmarshal(refundSubmitted.Value, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, event.GetType())
	assert.Equal(t, "rf-e2e-1", event.GetGatewayPayoutId())
	assert.Equal(t, "coingate", event.GetProvider())
	assert.Equal(t, 1, gateway.refundCalls)
	assert.Equal(t, 1, gateway.createCalls, "a refund never reaches the payout API")

	time.Sleep(1100 * time.Millisecond)
	resolved, err = worker.Sweep(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, resolved)
	refundConfirmed := pollMaybe(t, consumer, 30*time.Second)
	require.NotNil(t, refundConfirmed, "the refund confirmed event must reach gateway.payout.events")
	require.NoError(t, proto.Unmarshal(refundConfirmed.Value, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, event.GetType())
	assert.Equal(t, "rf-tx-1", event.GetTxid())

	var refundRow storage.ProviderPayoutRow
	require.NoError(t, db.Where("payout_id = ?", refundPayoutID).First(&refundRow).Error)
	require.NotNil(t, refundRow.RefundInvoiceID)
	assert.Equal(t, refundSourceID, *refundRow.RefundInvoiceID)

	// A replay of the refund command calls no provider and publishes nothing.
	replayRefund := &eventsv1.PayoutCommand{
		PayoutId:        refundPayoutID,
		Amount:          &commonv1.Money{MinorUnits: 50_000, Asset: "USDT", Decimals: 2},
		Network:         "trc20",
		Address:         "TXyzAddress0123456789",
		IdempotencyKey:  "payout:" + refundPayoutID + ":command",
		RefundInvoiceId: refundSourceID,
	}
	replayBody, err := proto.Marshal(replayRefund)
	require.NoError(t, err)
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
		Topic: usecase.TopicPayoutCommands,
		Key:   []byte(refundPayoutID),
		Value: replayBody,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
		},
	}).FirstErr())
	require.Eventually(t, func() bool {
		var count int64
		require.NoError(t, db.Model(&inbox.Record{}).Where("consumer = ?", group).Count(&count).Error)
		return count == 4
	}, 20*time.Second, 200*time.Millisecond, "the refund replay must reach the inbox")
	assert.Equal(t, 1, gateway.refundCalls, "the recorded refund never reaches the provider again")
	assert.Nil(t, pollMaybe(t, consumer, 2*time.Second), "the refund replay must not publish an event")
}
