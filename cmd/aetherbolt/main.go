package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/shubhamjain/aetherbolt/internal/function"
	"github.com/shubhamjain/aetherbolt/internal/httpapi"
	"github.com/shubhamjain/aetherbolt/internal/runtime"
	"github.com/shubhamjain/aetherbolt/internal/store"
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
	runtimes := runtime.NewBuiltinRegistry()
	service := function.NewService(repo, runtimes)
	server := &http.Server{
		Addr:              envOrDefault("AETHERBOLT_ADDR", ":8080"),
		Handler:           httpapi.NewHandler(service, logger),
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
