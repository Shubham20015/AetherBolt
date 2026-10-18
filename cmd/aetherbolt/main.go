package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	asyncsvc "github.com/shubhamjain/aetherbolt/internal/async"
	"github.com/shubhamjain/aetherbolt/internal/function"
	"github.com/shubhamjain/aetherbolt/internal/httpapi"
	"github.com/shubhamjain/aetherbolt/internal/queue"
	"github.com/shubhamjain/aetherbolt/internal/runtime"
	"github.com/shubhamjain/aetherbolt/internal/store"
	"github.com/shubhamjain/aetherbolt/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	databaseURL := os.Getenv("AETHERBOLT_DATABASE_URL")
	if databaseURL == "" {
		logger.Error("AETHERBOLT_DATABASE_URL is required")
		os.Exit(1)
	}

	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		logger.Error("invalid database URL", "error", err)
		os.Exit(1)
	}
	db := sql.OpenDB(stdlib.GetConnector(*config))
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		logger.Error("database unavailable", "error", err)
		os.Exit(1)
	}

	repo := store.NewPostgresFunctionStore(db)
	runtimes := runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner()))
	// Synchronous and asynchronous execution get independent admission budgets so
	// neither plane can starve the other; both wrap the same stateless runtime.
	syncScheduler, err := worker.NewManager(runtimes, worker.Config{
		MaxConcurrent: envIntOrDefault("AETHERBOLT_MAX_CONCURRENT", 4),
		QueueCapacity: envIntOrDefault("AETHERBOLT_QUEUE_CAPACITY", 16),
	})
	if err != nil {
		logger.Error("invalid synchronous scheduler configuration", "error", err)
		os.Exit(1)
	}
	asyncScheduler, err := worker.NewManager(runtimes, worker.Config{
		MaxConcurrent: envIntOrDefault("AETHERBOLT_ASYNC_MAX_CONCURRENT", 4),
		QueueCapacity: envIntOrDefault("AETHERBOLT_ASYNC_QUEUE_CAPACITY", 16),
	})
	if err != nil {
		logger.Error("invalid asynchronous scheduler configuration", "error", err)
		os.Exit(1)
	}
	service := function.NewService(repo, syncScheduler)
	kafkaClient := queue.NewKafka([]string{envOrDefault("AETHERBOLT_KAFKA_BROKER", "localhost:9092")}, envOrDefault("AETHERBOLT_KAFKA_TOPIC", "aetherbolt.invocations"))
	defer kafkaClient.Close()
	asyncService := asyncsvc.New(asyncsvc.NewPostgresStore(db), kafkaClient, asyncScheduler)
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			// Recover invocations stranded by a crashed worker, then publish any
			// due work (including deferred retries and DLQ records).
			if err := asyncService.Reconcile(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("invocation reconcile failed", "error", err)
			}
			if err := asyncService.Republish(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("outbox republish failed", "error", err)
			}
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	// Several consumers share one group so partitions are processed in parallel;
	// each is supervised so a transient error cannot stop the data plane.
	consumerCount := envIntOrDefault("AETHERBOLT_ASYNC_CONSUMERS", 4)
	if consumerCount < 1 {
		consumerCount = 1
	}
	for i := 0; i < consumerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			superviseConsumer(workerCtx, logger, kafkaClient, "aetherbolt-workers", asyncService.Process)
		}()
	}
	server := &http.Server{
		Addr:              envOrDefault("AETHERBOLT_ADDR", ":8080"),
		Handler:           httpapi.NewHandler(service, logger).WithAsync(asyncService),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	syncScheduler.BeginDrain()
	asyncScheduler.BeginDrain()
	stopWorkers()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	if err := syncScheduler.Wait(shutdownCtx); err != nil {
		logger.Error("synchronous execution drain failed", "error", err)
	}
	if err := asyncScheduler.Wait(shutdownCtx); err != nil {
		logger.Error("asynchronous execution drain failed", "error", err)
	}
	workers.Wait()
}

// superviseConsumer restarts a consumer with backoff so a transient broker or
// database error cannot permanently stop asynchronous processing. A long
// healthy run resets the backoff.
func superviseConsumer(ctx context.Context, logger *slog.Logger, consumer *queue.Kafka, group string, handle func(context.Context, string) error) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := consumer.Consume(ctx, group, handle)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		logger.Warn("kafka consumer stopped; restarting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			if backoff *= 2; backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func envIntOrDefault(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return parsed
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
