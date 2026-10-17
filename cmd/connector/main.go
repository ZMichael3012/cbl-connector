// main starts the HTTP and gRPC service of the CBL connector platform:
// the pipeline that normalizes provider webhooks into canonical events.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/cbl/cbl-kit/ginmw"
	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/gateway"
	"github.com/cbl/cbl-connector/internal/providers/btcpay"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/transport"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// main starts the administrative HTTP server, the gRPC server, the outbox relay,
// and the webhook normalization consumer; it shuts down on SIGINT/SIGTERM.
// ConnectorService registration will be added in task 3.2 together with the provider adapter.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Error("PG_DSN is required")
		return
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Error("connect postgres failed", zap.Error(err))
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	migrationsDir := getenv("MIGRATIONS_DIR", "./migrations")
	if migrateErr := storagex.Migrate(ctx, db, migrationsDir); migrateErr != nil {
		log.Error("apply migrations failed", zap.String("dir", migrationsDir), zap.Error(migrateErr))
		return
	}

	// Provider manifests (17.2, SPI v1): the versioned registry is the source of
	// truth for known providers; adapters plug into the core through it and the
	// SPI interfaces without core changes (functional extensibility guarantee).
	manifestRegistry, err := usecase.NewManifestRegistry(domain.BuiltinManifests()...)
	if err != nil {
		log.Error("build provider manifest registry failed", zap.Error(err))
		return
	}
	log.Info("provider manifests loaded", zap.Int("providers", len(manifestRegistry.IDs())))

	// Provider adapters (17.6 - 17.9): the BTCPay adapter is the first real
	// provider (dev/CI contour). Without CONNECTOR_BTCPAY_BASE_URL the runtime
	// stays fail-closed: no gateways, the unavailable selector and no fetcher.
	gateways := map[string]domain.PaymentGateway{}
	if base := strings.TrimSpace(os.Getenv("CONNECTOR_BTCPAY_BASE_URL")); base != "" {
		capabilities, capsErr := capabilitiesFromEnv(os.Getenv("CONNECTOR_BTCPAY_CAPABILITIES"))
		if capsErr != nil {
			log.Error("parse CONNECTOR_BTCPAY_CAPABILITIES failed", zap.Error(capsErr))
			return
		}
		adapter, adapterErr := btcpay.New(btcpay.Config{
			BaseURL:       base,
			StoreID:       os.Getenv("CONNECTOR_BTCPAY_STORE_ID"),
			APIToken:      os.Getenv("CONNECTOR_BTCPAY_API_TOKEN"),
			WebhookSecret: []byte(os.Getenv("CONNECTOR_BTCPAY_WEBHOOK_SECRET")),
			Capabilities:  capabilities,
			Timeout:       durationEnv(log, "CONNECTOR_BTCPAY_TIMEOUT", 10*time.Second),
		})
		if adapterErr != nil {
			log.Error("build btcpay adapter failed", zap.Error(adapterErr))
			return
		}
		// Every external call goes through the shared resilience stack (plan
		// 14.5.3): bulkhead, retries with jitter, breaker and attempt timeouts.
		// The manifest limiter of the provider is wired when it configures a
		// rate limit (the BTCPay stand declares none).
		gateways["btcpay"] = gateway.New(adapter, gateway.ConfigFromEnv(os.Getenv), gateway.WithLogger(log))
		log.Info("btcpay adapter configured", zap.String("base_url", base))
	}

	decoders := make([]usecase.WebhookDecoder, 0, 2)
	for _, provider := range splitCSV(getenv("CONNECTOR_SANDBOX_PROVIDERS", "mock")) {
		decoder, decodeErr := usecase.NewSandboxDecoder(provider)
		if decodeErr != nil {
			log.Error("build sandbox decoder failed", zap.String("provider", provider), zap.Error(decodeErr))
			return
		}
		decoders = append(decoders, decoder)
	}
	if len(decoders) == 0 {
		log.Error("CONNECTOR_SANDBOX_PROVIDERS is empty: no webhook decoders configured")
		return
	}
	registry, err := usecase.NewDecoderRegistry(decoders...)
	if err != nil {
		log.Error("build decoder registry failed", zap.Error(err))
		return
	}
	// The provider invoice mapping resolves platform payments of real provider
	// webhooks (17.2, T-CONNECTOR-03); fetch_required deliveries resolve their
	// status through the registered adapters (17.9).
	invoiceMapping := storage.NewProviderInvoicesRepository(db)
	normalizerOptions := []usecase.NormalizerOption{
		usecase.WithPaymentMapping(invoiceMapping),
		usecase.WithFetchMandatory(func(provider string) bool {
			manifest, ok := manifestRegistry.Lookup(provider)
			return ok && manifest.FetchMandatory
		}),
	}
	if len(gateways) > 0 {
		fetcher, fetchErr := usecase.NewGatewayStatusFetcher(gateways)
		if fetchErr != nil {
			log.Error("build provider status fetcher failed", zap.Error(fetchErr))
			return
		}
		normalizerOptions = append(normalizerOptions, usecase.WithPaymentFetcher(fetcher))
	}
	normalizer := usecase.NewNormalizer(registry, log, normalizerOptions...)

	// Routing rules (3.4): PostgreSQL is the source of truth; Redis is only a
	// cache, and its absence or failure does not affect the selection. The provider
	// registry and the Router are wired in task 3.2 (KI-01).
	ruleStore := storage.NewRouteRulesRepository(db)
	var ruleCache usecase.RouteConfigCacheStore
	var capabilityCache usecase.CapabilityCache
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		redisClient := redis.NewClient(&redis.Options{
			Addr:                  redisAddr,
			Password:              os.Getenv("REDIS_PASSWORD"),
			DialTimeout:           200 * time.Millisecond,
			ReadTimeout:           100 * time.Millisecond,
			WriteTimeout:          100 * time.Millisecond,
			ContextTimeoutEnabled: true,
		})
		defer func() { _ = redisClient.Close() }()
		ruleCache = storage.NewRouteConfigCache(redisClient)
		capabilityCache = storage.NewCapabilitiesCache(redisClient)
	}
	ruleSource := usecase.NewCachedRouteRules(ruleStore, ruleCache, usecase.RouteCacheTTLFromEnv(os.Getenv), log)
	if rules, loadErr := ruleSource.Load(ctx); loadErr != nil {
		log.Warn("routing rules load failed", zap.Error(loadErr))
	} else {
		log.Info("routing rules loaded", zap.Int("rules", len(rules)))
	}

	// The admin contour (CBL-12.2): routing configuration revision and manual
	// provider disable; the manifest registry (17.2) supplies the known-provider
	// set of the disable check.
	adminRouting := usecase.NewAdminRouting(ruleStore, ruleSource, manifestRegistry.IDs, log)

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		brokerList := strings.Split(brokers, ",")
		publisher, pubErr := kafkapub.New(brokerList)
		if pubErr != nil {
			log.Error("kafka publisher failed", zap.Error(pubErr))
			return
		}
		defer publisher.Close()

		relay := outbox.NewRelay(db, publisher, log)
		go func() {
			if runErr := relay.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("outbox relay stopped", zap.Error(runErr))
			}
		}()

		group := getenv("KAFKA_CONSUMER_GROUP", "cbl-connector-webhooks")
		processor := inbox.NewProcessor(group, db, inbox.NewGuard(), normalizer, publisher,
			inbox.NewRouter(usecase.TopicGatewayWebhooksRaw, 3), log)
		topics := []string{
			usecase.TopicGatewayWebhooksRaw,
			usecase.TopicGatewayWebhooksRaw + inbox.RetrySuffix5s,
			usecase.TopicGatewayWebhooksRaw + inbox.RetrySuffix1m,
			usecase.TopicGatewayWebhooksRaw + inbox.RetrySuffix10m,
		}
		runner, runErr := kafkaconsumer.New(brokerList, group, topics, processor, log)
		if runErr != nil {
			log.Error("webhook consumer failed", zap.Error(runErr))
			return
		}
		defer runner.Close()
		go func() {
			if consumeErr := runner.Run(ctx); consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
				log.Error("webhook consumer stopped", zap.Error(consumeErr))
			}
		}()
		log.Info("webhook normalizer started", zap.String("group", group))
	} else {
		log.Warn("KAFKA_BROKERS is empty: webhook normalizer disabled")
	}

	httpAddr := getenv("HTTP_ADDR", ":8050")
	grpcAddr := getenv("GRPC_ADDR", ":9050")
	metricsAddr := getenv("METRICS_ADDR", ":9250")

	router := gin.New()
	router.Use(ginmw.RequestID())
	router.Use(ginmw.AccessLog(log))
	router.Use(ginmw.Recovery(log))
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<20),
		grpc.UnaryInterceptor(grpcmw.UnaryServer(log)),
	)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	connectorv1.RegisterConnectorAdminServiceServer(grpcSrv, transport.NewAdminServer(adminRouting, log))

	// The money-path ConnectorService (17.2): with a configured adapter the
	// interim static selector (17.9) routes invoices to it; without adapters
	// the calls fail closed with UNAVAILABLE or NotFound instead of
	// UNIMPLEMENTED, so consumers can integrate today.
	capabilitySources := make([]usecase.CapabilitySource, 0, len(gateways))
	for _, gateway := range gateways {
		if source, ok := gateway.(usecase.CapabilitySource); ok {
			capabilitySources = append(capabilitySources, source)
		}
	}
	capabilitiesLoader, err := usecase.NewCapabilitiesLoader(capabilitySources, capabilityCache, time.Hour, log)
	if err != nil {
		log.Error("build capabilities loader failed", zap.Error(err))
		return
	}
	var selector domain.Router = unavailableSelector{}
	if len(gateways) > 0 {
		selector, err = usecase.NewStaticSelector(gateways)
		if err != nil {
			log.Error("build provider selector failed", zap.Error(err))
			return
		}
	}
	connectorServer := transport.NewConnectorServer(
		selector,
		gateways,
		invoiceMapping,
		capabilitiesLoader,
		log,
	)
	connectorv1.RegisterConnectorServiceServer(grpcSrv, connectorServer)
	reflection.Register(grpcSrv)

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("grpc listen failed", zap.String("addr", grpcAddr), zap.Error(err))
		return
	}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("grpc server started", zap.String("addr", grpcAddr))
		if serveErr := grpcSrv.Serve(grpcListener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	log.Info("shutdown finished")
}

// unavailableSelector fails closed until the router and adapters are wired
// (17.6 - 17.9): an invoice request never silently falls through.
type unavailableSelector struct{}

// Select implements transport.GatewaySelector.
func (unavailableSelector) Select(context.Context, domain.RouteRequest) (domain.PaymentGateway, error) {
	return nil, domain.ErrProviderUnavailable
}

// metricsHandler serves a metrics stub until task 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// getenv returns the environment variable value or the fallback value.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// splitCSV parses a comma-separated list of values from an environment variable.
func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

// capabilityEnvRow describes one network row of the adapter capabilities env.
type capabilityEnvRow struct {
	Network       string `json:"network"`
	Asset         string `json:"asset"`
	MinMinor      int64  `json:"min_minor"`
	MaxMinor      int64  `json:"max_minor"`
	Decimals      uint32 `json:"decimals"`
	Confirmations uint32 `json:"confirmations"`
}

// capabilitiesFromEnv parses the adapter capability rows of the deployment
// (the catalog artifact 17.21 replaces this configuration later); unknown
// fields, duplicate networks, missing decimals and inverted bounds fail fast
// (spec E-7).
func capabilitiesFromEnv(raw string) (domain.Capabilities, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var rows []capabilityEnvRow
	if err := decoder.Decode(&rows); err != nil {
		return domain.Capabilities{}, fmt.Errorf("capabilities env: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.Capabilities{}, errors.New("capabilities env: unexpected trailing json")
	}
	if len(rows) == 0 {
		return domain.Capabilities{}, errors.New("capabilities env: no rows")
	}
	networks := make(map[string]domain.NetworkCapability, len(rows))
	for _, row := range rows {
		network := strings.TrimSpace(row.Network)
		asset := strings.ToUpper(strings.TrimSpace(row.Asset))
		if network == "" || asset == "" {
			return domain.Capabilities{}, errors.New("capabilities env: network and asset are required")
		}
		if row.Decimals == 0 {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: decimals are required for network %q", network)
		}
		if row.MinMinor < 0 || row.MaxMinor < 0 {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: negative bounds for network %q", network)
		}
		if row.MaxMinor != 0 && row.MaxMinor < row.MinMinor {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: max below min for network %q", network)
		}
		if _, duplicate := networks[network]; duplicate {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: duplicate network %q", network)
		}
		minAmount, err := money.New(row.MinMinor, asset, row.Decimals)
		if err != nil {
			return domain.Capabilities{}, fmt.Errorf("capabilities env: min amount: %w", err)
		}
		maximum := money.Money{Asset: asset, Decimals: row.Decimals}
		if row.MaxMinor != 0 {
			maximum, err = money.New(row.MaxMinor, asset, row.Decimals)
			if err != nil {
				return domain.Capabilities{}, fmt.Errorf("capabilities env: max amount: %w", err)
			}
		}
		networks[network] = domain.NetworkCapability{
			Asset:             asset,
			MinAmount:         minAmount,
			MaxAmount:         maximum,
			ConfirmationDepth: row.Confirmations,
		}
	}
	return domain.Capabilities{Networks: networks}, nil
}

// durationEnv reads a duration from an environment variable; an invalid value
// falls back to the default with a warning.
func durationEnv(log *zap.Logger, key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		log.Warn("invalid duration env, using default",
			zap.String("key", key),
			zap.String("value", raw),
			zap.Duration("default", fallback),
		)
		return fallback
	}
	return parsed
}
