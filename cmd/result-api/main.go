package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Alphasxd/AgentStorm/internal/results"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("result API stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, config.databaseURL)
	if err != nil {
		return fmt.Errorf("configure PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	if err := results.ApplyMigrations(ctx, pool); err != nil {
		return err
	}

	objectStore, err := results.NewMinIOStore(results.MinIOConfig{
		Endpoint:  config.s3Endpoint,
		AccessKey: config.s3AccessKey,
		SecretKey: config.s3SecretKey,
		Bucket:    config.s3Bucket,
		Region:    config.s3Region,
		UseSSL:    config.s3UseSSL,
	})
	if err != nil {
		return err
	}
	if err := objectStore.EnsureBucket(ctx, config.s3Region); err != nil {
		return err
	}

	service := results.NewService(results.NewPostgresRepository(pool), objectStore)
	handler, err := results.NewHTTPHandler(service, results.HTTPConfig{
		WriteToken: config.writeToken,
		ReadToken:  config.readToken,
	})
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              config.listenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       35 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("result API listening", "address", config.listenAddress)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		return nil
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	}
}

type configuration struct {
	listenAddress string
	databaseURL   string
	s3Endpoint    string
	s3AccessKey   string
	s3SecretKey   string
	s3Bucket      string
	s3Region      string
	s3UseSSL      bool
	writeToken    string
	readToken     string
}

func loadConfig() (configuration, error) {
	useSSL, err := strconv.ParseBool(environment("AGENTSTORM_S3_USE_SSL", "false"))
	if err != nil {
		return configuration{}, fmt.Errorf("AGENTSTORM_S3_USE_SSL must be true or false")
	}
	config := configuration{
		listenAddress: environment("AGENTSTORM_LISTEN_ADDR", ":8080"),
		databaseURL:   os.Getenv("AGENTSTORM_DATABASE_URL"),
		s3Endpoint:    os.Getenv("AGENTSTORM_S3_ENDPOINT"),
		s3AccessKey:   os.Getenv("AGENTSTORM_S3_ACCESS_KEY"),
		s3SecretKey:   os.Getenv("AGENTSTORM_S3_SECRET_KEY"),
		s3Bucket:      environment("AGENTSTORM_S3_BUCKET", "agentstorm-results"),
		s3Region:      environment("AGENTSTORM_S3_REGION", "us-east-1"),
		s3UseSSL:      useSSL,
		writeToken:    os.Getenv("AGENTSTORM_RESULT_WRITE_TOKEN"),
		readToken:     os.Getenv("AGENTSTORM_RESULT_READ_TOKEN"),
	}
	for name, value := range map[string]string{
		"AGENTSTORM_DATABASE_URL":       config.databaseURL,
		"AGENTSTORM_S3_ENDPOINT":        config.s3Endpoint,
		"AGENTSTORM_S3_ACCESS_KEY":      config.s3AccessKey,
		"AGENTSTORM_S3_SECRET_KEY":      config.s3SecretKey,
		"AGENTSTORM_RESULT_WRITE_TOKEN": config.writeToken,
		"AGENTSTORM_RESULT_READ_TOKEN":  config.readToken,
	} {
		if value == "" {
			return configuration{}, fmt.Errorf("%s is required", name)
		}
	}
	return config, nil
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
