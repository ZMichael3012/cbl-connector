// main starts the HTTP and gRPC service of the CBL connector platform:
// the pipeline that normalizes provider webhooks into canonical events.
package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"

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
	normalizer := usecase.NewNormalizer(registry, log)

	// Routing rules (3.4): PostgreSQL is the source of truth; Redis is only a
	// cache, and its absence or failure does not affect the selection. The provider
	// registry and the Router are wired in task 3.2 (KI-01).
	ruleStore := storage.NewRouteRulesRepository(db)
	var ruleCache usecase.RouteConfigCacheStore
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
	}
	ruleSource := usecase.NewCachedRouteRules(ruleStore, ruleCache, usecase.RouteCacheTTLFromEnv(os.Getenv), log)
	if rules, loadErr := ruleSource.Load(ctx); loadErr != nil {
		log.Warn("routing rules load failed", zap.Error(loadErr))
	} else {
		log.Info("routing rules loaded", zap.Int("rules", len(rules)))
	}

	// The admin contour (CBL-12.2): routing configuration revision and manual
	// provider disable; the provider registry of the union check arrives with
	// task 3.2 (KI-01), the rules themselves are known today.
	adminRouting := usecase.NewAdminRouting(ruleStore, ruleSource, nil, log)

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
