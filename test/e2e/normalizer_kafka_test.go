//go:build e2e

// Пакет e2e содержит сквозной сценарий нормализатора connector: сырой вебхук
// из Kafka превращается в каноническое событие, дубликаты подавляются,
// неподписанные события уходят в DLQ. Стенд: реальные Kafka и PostgreSQL.
package e2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-connector/internal/usecase"
)

// Топики стенда: вход, канонический выход и DLQ.
const (
	rawTopic       = "gateway.webhooks.raw"
	canonicalTopic = "gateway.events.canonical"
	dlqTopic       = rawTopic + ".dlq"
)

// e2eBody - валидный вебхук sandbox-провайдера из спеки CBL-3.5.
const e2eBody = `{
  "payment_id": "01a138fd-0000-7000-8000-000000000003", "gateway_id": "inv-e2e", "status": "confirmed",
  "received_amount": {"minor_units": 250000, "asset": "USDT", "decimals": 2},
  "confirmations": 5, "txid": "0xe2e", "network": "trc20",
  "event_ts": "2026-10-14T12:00:05Z"
}`

// e2ePaymentID задает UUID платежа из e2eBody.
const e2ePaymentID = "01a138fd-0000-7000-8000-000000000003"

func setupPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("connector_e2e"),
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
	return db
}

func setupKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-connector-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topics := []string{
		rawTopic, rawTopic + ".retry.5s", rawTopic + ".retry.1m", rawTopic + ".retry.10m", dlqTopic,
		canonicalTopic,
	}
	for _, topic := range topics {
		_, err := admin.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err, topic)
	}
	return brokers
}

// rawRecord собирает Kafka-сообщение сырого вебхука.
func rawRecord(t *testing.T, eventID, body string, verified bool) *kgo.Record {
	t.Helper()
	raw := &eventsv1.GatewayWebhookRaw{
		Provider:          "mock",
		GatewayId:         "inv-e2e",
		ReceivedAt:        timestamppb.New(time.Now().UTC()),
		RawBody:           []byte(body),
		SignatureVerified: verified,
	}
	payload, err := proto.Marshal(raw)
	require.NoError(t, err)
	return &kgo.Record{
		Topic: rawTopic,
		Key:   []byte("mock:inv-e2e"),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(eventID)},
			{Key: outbox.HeaderEventType, Value: []byte("gateway.webhook.raw")},
			{Key: outbox.HeaderProducer, Value: []byte("cbl-webhook-ingress")},
		},
	}
}

// pollRecord ждет записи в топике до timeout; PollFetches вызывается с коротким
// дедлайном, иначе при отсутствии сообщений он блокируется бессрочно.
func pollRecord(t *testing.T, consumer *kgo.Client, timeout time.Duration) *kgo.Record {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := consumer.PollFetches(pollCtx)
		cancel()
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

func TestE2ENormalizerPipeline(t *testing.T) {
	db := setupPostgres(t)
	brokers := setupKafka(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	decoder, err := usecase.NewSandboxDecoder("mock")
	require.NoError(t, err)
	registry, err := usecase.NewDecoderRegistry(decoder)
	require.NoError(t, err)
	normalizer := usecase.NewNormalizer(registry, zap.NewNop())
	processor := inbox.NewProcessor(
		"cbl-connector-e2e", db, inbox.NewGuard(), normalizer, publisher,
		inbox.NewRouter(rawTopic, 3), zap.NewNop(),
	)

	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithInterval(50*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	runner, err := kafkaconsumer.New(brokers, "cbl-connector-e2e",
		[]string{rawTopic, rawTopic + ".retry.5s", rawTopic + ".retry.1m", rawTopic + ".retry.10m"},
		processor, zap.NewNop())
	require.NoError(t, err)
	defer runner.Close()
	go func() { _ = runner.Run(ctx) }()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	canonicalConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(canonicalTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer canonicalConsumer.Close()

	// Шаг 1: валидный вебхук превращается в каноническое событие.
	firstID := uuid.Must(uuid.NewV7()).String()
	require.NoError(t, producer.ProduceSync(ctx, rawRecord(t, firstID, e2eBody, true)).FirstErr())

	record := pollRecord(t, canonicalConsumer, 45*time.Second)
	require.NotNil(t, record, "каноническое событие не доставлено за 45 секунд")
	assert.Equal(t, e2ePaymentID, string(record.Key), "ключ партиции - payment_id")

	var payload eventsv1.CanonicalPaymentEvent
	require.NoError(t, proto.Unmarshal(record.Value, &payload))
	assert.Equal(t, e2ePaymentID, payload.GetPaymentId())
	assert.Equal(t, "inv-e2e", payload.GetGatewayId())
	assert.Equal(t, "mock", payload.GetProvider())
	assert.Equal(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, payload.GetType())
	assert.Equal(t, int64(250_000), payload.GetReceivedAmount().GetMinorUnits())
	assert.Equal(t, uint32(5), payload.GetConfirmations())
	assert.Equal(t, "0xe2e", payload.GetTxid())

	// Шаг 2: повторная доставка того же raw-события не создает второй конверт.
	require.NoError(t, producer.ProduceSync(ctx, rawRecord(t, firstID, e2eBody, true)).FirstErr())
	assert.Nil(t, pollRecord(t, canonicalConsumer, 3*time.Second), "дубликат не должен публиковаться")
	var outboxCount, inboxCount int64
	require.NoError(t, db.Model(&outbox.Record{}).Count(&outboxCount).Error)
	require.NoError(t, db.Table("inbox").Count(&inboxCount).Error)
	assert.Equal(t, int64(1), outboxCount, "дубликат не создает второй конверт в outbox")
	assert.Equal(t, int64(1), inboxCount)

	// Шаг 3: неподписанный вебхук уходит в DLQ, каноническое событие не публикуется.
	unsignedID := uuid.Must(uuid.NewV7()).String()
	require.NoError(t, producer.ProduceSync(ctx, rawRecord(t, unsignedID, e2eBody, false)).FirstErr())

	dlqConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(dlqTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer dlqConsumer.Close()

	dlqRecord := pollRecord(t, dlqConsumer, 45*time.Second)
	require.NotNil(t, dlqRecord, "неподписанный вебхук не доставлен в DLQ за 45 секунд")
	assert.Equal(t, "mock:inv-e2e", string(dlqRecord.Key))
	var errorHeader string
	for _, header := range dlqRecord.Headers {
		if header.Key == inbox.HeaderError {
			errorHeader = string(header.Value)
		}
	}
	assert.Contains(t, errorHeader, "signature not verified")
	assert.Nil(t, pollRecord(t, canonicalConsumer, 2*time.Second), "неподписанный вебхук не нормализуется")

	// Шаг 4: poison pill с гигантским event_ts (security review) уходит в DLQ
	// с ограниченной ошибкой, и конвейер не блокируется: следующее валидное
	// событие доходит до canonical.
	poisonID := uuid.Must(uuid.NewV7()).String()
	poisonBody := `{"payment_id":"` + e2ePaymentID + `","gateway_id":"inv-e2e","status":"confirmed",` +
		`"received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"` +
		strings.Repeat("2", 600*(1<<10)) + `"}`
	require.NoError(t, producer.ProduceSync(ctx, rawRecord(t, poisonID, poisonBody, true)).FirstErr())
	poisonRecord := pollRecord(t, dlqConsumer, 45*time.Second)
	require.NotNil(t, poisonRecord, "poison pill не доставлен в DLQ за 45 секунд")

	recoveryBody := strings.Replace(e2eBody, `"status": "confirmed"`, `"status": "detected"`, 1)
	require.NoError(t, producer.ProduceSync(ctx, rawRecord(t, uuid.Must(uuid.NewV7()).String(), recoveryBody, true)).FirstErr())
	recovery := pollRecord(t, canonicalConsumer, 45*time.Second)
	require.NotNil(t, recovery, "конвейер заблокирован poison pill")
}
