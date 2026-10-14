//go:build integration

package usecase_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// integrationPaymentID повторяет UUID платежа из sandboxValidBody.
const integrationPaymentID = sandboxPaymentID

// setupNormalizerDB запускает PostgreSQL, применяет миграции и собирает Processor.
func setupNormalizerDB(t *testing.T) (*gorm.DB, *inbox.Processor) {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("connector"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))

	decoder, err := usecase.NewSandboxDecoder("mock")
	require.NoError(t, err)
	registry, err := usecase.NewDecoderRegistry(decoder)
	require.NoError(t, err)
	normalizer := usecase.NewNormalizer(registry, zap.NewNop())
	processor := inbox.NewProcessor(
		"cbl-connector-webhooks", db, inbox.NewGuard(), normalizer, nil,
		inbox.NewRouter(usecase.TopicGatewayWebhooksRaw, 3), zap.NewNop(),
	)
	return db, processor
}

// marshalRaw собирает payload сырого вебхука для сообщения.
func marshalRaw(t *testing.T, provider string, signatureVerified bool, body string) []byte {
	t.Helper()
	raw := &eventsv1.GatewayWebhookRaw{
		Provider:          provider,
		GatewayId:         "inv-1",
		ReceivedAt:        timestamppb.New(time.Now().UTC()),
		RawBody:           []byte(body),
		SignatureVerified: signatureVerified,
	}
	payload, err := proto.Marshal(raw)
	require.NoError(t, err)
	return payload
}

// rawMessage собирает сообщение консьюмер-конверта с сырым вебхуком.
func rawMessage(t *testing.T, eventID string, payload []byte) inbox.Message {
	t.Helper()
	return inbox.Message{
		Topic: usecase.TopicGatewayWebhooksRaw,
		Key:   "mock:inv-1",
		Headers: map[string]string{
			outbox.HeaderEventID:   eventID,
			outbox.HeaderEventType: "gateway.webhook.raw",
		},
		Value: payload,
	}
}

func TestIntegrationNormalizerPublishesCanonical(t *testing.T) {
	db, processor := setupNormalizerDB(t)
	ctx := context.Background()
	eventID := "01a138fd-0000-7000-8000-000000000001"

	err := processor.Process(ctx, rawMessage(t, eventID, marshalRaw(t, "mock", true, sandboxValidBody)))
	require.NoError(t, err)

	var records []outbox.Record
	require.NoError(t, db.Find(&records).Error)
	require.Len(t, records, 1)
	record := records[0]
	assert.Equal(t, usecase.TopicGatewayEventsCanonical, record.Topic)
	assert.Equal(t, integrationPaymentID, record.Key)
	assert.Equal(t, usecase.ProducerConnector, record.Headers[outbox.HeaderProducer])
	assert.Equal(t, "gateway.payment.confirmed", record.Headers[outbox.HeaderEventType])

	var payload eventsv1.CanonicalPaymentEvent
	require.NoError(t, proto.Unmarshal(record.Payload, &payload))
	assert.Equal(t, integrationPaymentID, payload.GetPaymentId())
	assert.Equal(t, "mock", payload.GetProvider())
	assert.Equal(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, payload.GetType())
	assert.Equal(t, int64(100_000), payload.GetReceivedAmount().GetMinorUnits())

	var inboxCount int64
	require.NoError(t, db.Table("inbox").Count(&inboxCount).Error)
	assert.Equal(t, int64(1), inboxCount)
}

func TestIntegrationNormalizerSkipsDuplicateRawEvent(t *testing.T) {
	db, processor := setupNormalizerDB(t)
	ctx := context.Background()
	eventID := "01a138fd-0000-7000-8000-000000000002"
	message := rawMessage(t, eventID, marshalRaw(t, "mock", true, sandboxValidBody))

	require.NoError(t, processor.Process(ctx, message))
	require.NoError(t, processor.Process(ctx, message), "повторная доставка подтверждается без эффекта")

	var outboxCount, inboxCount int64
	require.NoError(t, db.Model(&outbox.Record{}).Count(&outboxCount).Error)
	require.NoError(t, db.Table("inbox").Count(&inboxCount).Error)
	assert.Equal(t, int64(1), outboxCount, "дубликат не создает второй конверт")
	assert.Equal(t, int64(1), inboxCount)
}

func TestIntegrationNormalizerRejections(t *testing.T) {
	db, processor := setupNormalizerDB(t)
	ctx := context.Background()

	unsigned := rawMessage(t, "01a138fd-0000-7000-8000-000000000003", marshalRaw(t, "mock", false, sandboxValidBody))
	require.ErrorIs(t, processor.Process(ctx, unsigned), domain.ErrUnsignedWebhook)

	unknown := rawMessage(t, "01a138fd-0000-7000-8000-000000000004", marshalRaw(t, "", true, sandboxValidBody))
	require.ErrorIs(t, processor.Process(ctx, unknown), domain.ErrUnknownProvider)

	broken := rawMessage(t, "01a138fd-0000-7000-8000-000000000005", marshalRaw(t, "mock", true, `{broken`))
	require.ErrorIs(t, processor.Process(ctx, broken), domain.ErrInvalidWebhook)

	badStatus := rawMessage(t, "01a138fd-0000-7000-8000-000000000006", marshalRaw(t, "mock", true,
		`{"payment_id":"p","gateway_id":"g","status":"weird","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`))
	require.ErrorIs(t, processor.Process(ctx, badStatus), domain.ErrUnknownState)

	// Poison pill из security review: гигантский event_ts отклоняется с короткой
	// ошибкой, retry-конверт остается маленьким и достижимым для DLQ.
	poison := rawMessage(t, "01a138fd-0000-7000-8000-000000000007", marshalRaw(t, "mock", true,
		`{"payment_id":"`+integrationPaymentID+`","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"`+
			strings.Repeat("2", 300*(1<<10))+`"}`))
	poisonErr := processor.Process(ctx, poison)
	require.ErrorIs(t, poisonErr, domain.ErrInvalidWebhook)
	assert.Less(t, len(poisonErr.Error()), 400, "текст ошибки ограничен, x-error не раздувается")

	var count int64
	require.NoError(t, db.Model(&outbox.Record{}).Count(&count).Error)
	assert.Zero(t, count, "отклоненные события не публикуются")
}
