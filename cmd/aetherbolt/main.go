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
	scheduler, err := worker.NewManager(runtimes, worker.Config{
		MaxConcurrent: envIntOrDefault("AETHERBOLT_MAX_CONCURRENT", 4),
		QueueCapacity: envIntOrDefault("AETHERBOLT_QUEUE_CAPACITY", 16),
	})
	if err != nil {
		logger.Error("invalid scheduler configuration", "error", err)
		os.Exit(1)
	}
	service := function.NewService(repo, scheduler)
	kafkaClient := queue.NewKafka([]string{envOrDefault("AETHERBOLT_KAFKA_BROKER", "localhost:9092")}, envOrDefault("AETHERBOLT_KAFKA_TOPIC", "aetherbolt.invocations"))
	defer kafkaClient.Close()
	asyncService := asyncsvc.New(asyncsvc.NewPostgresStore(db), kafkaClient, scheduler)
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
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
	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := kafkaClient.Consume(workerCtx, "aetherbolt-workers", asyncService.Process); err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn("kafka consumer stopped", "error", err)
		}
	}()
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
	scheduler.BeginDrain()
	stopWorkers()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	if err := scheduler.Wait(shutdownCtx); err != nil {
		logger.Error("execution drain failed", "error", err)
	}
	workers.Wait()
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
