package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

	service := results.NewServiceWithLimitPolicy(results.NewPostgresRepository(pool), objectStore, config.limitPolicy)
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
	limitPolicy   results.LimitPolicy
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
	globalConcurrency, err := boundedNonNegativeInteger("AGENTSTORM_GLOBAL_MAX_CONCURRENCY", 0, 100000)
	if err != nil {
		return configuration{}, err
	}
	globalRate, err := boundedNonNegativeInteger("AGENTSTORM_GLOBAL_REQUESTS_PER_MINUTE", 0, 1000000)
	if err != nil {
		return configuration{}, err
	}
	leaseSeconds, err := boundedNonNegativeInteger("AGENTSTORM_PERMIT_LEASE_SECONDS", 30, 300)
	if err != nil || leaseSeconds < 10 {
		return configuration{}, fmt.Errorf("AGENTSTORM_PERMIT_LEASE_SECONDS must be between 10 and 300")
	}
	providerLimits := map[string]results.Limit{}
	if raw := os.Getenv("AGENTSTORM_PROVIDER_LIMITS_JSON"); raw != "" {
		providerLimits, err = decodeProviderLimits(raw)
		if err != nil {
			return configuration{}, fmt.Errorf("AGENTSTORM_PROVIDER_LIMITS_JSON must be a provider-to-limit JSON object")
		}
	}
	for provider, limit := range providerLimits {
		if provider == "" || provider != strings.TrimSpace(provider) || len(provider) > 128 || strings.ContainsRune(provider, '\x00') ||
			limit.MaxConcurrency < 0 || limit.MaxConcurrency > 100000 ||
			limit.RequestsPerMinute < 0 || limit.RequestsPerMinute > 1000000 {
			return configuration{}, fmt.Errorf("AGENTSTORM_PROVIDER_LIMITS_JSON contains an invalid limit")
		}
	}
	config.limitPolicy = results.LimitPolicy{
		Global:    results.Limit{MaxConcurrency: globalConcurrency, RequestsPerMinute: globalRate},
		Providers: providerLimits, LeaseDuration: time.Duration(leaseSeconds) * time.Second,
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

func decodeProviderLimits(raw string) (map[string]results.Limit, error) {
	var entries map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	if err := decoder.Decode(&entries); err != nil || entries == nil {
		return nil, errors.New("provider limits must be a JSON object")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	limits := make(map[string]results.Limit, len(entries))
	for provider, payload := range entries {
		var limit results.Limit
		entryDecoder := json.NewDecoder(bytes.NewReader(payload))
		entryDecoder.DisallowUnknownFields()
		if err := entryDecoder.Decode(&limit); err != nil || bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			return nil, errors.New("provider limit is invalid")
		}
		if err := requireJSONEOF(entryDecoder); err != nil {
			return nil, err
		}
		limits[provider] = limit
	}
	return limits, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func boundedNonNegativeInteger(name string, fallback, maximum int) (int, error) {
	value, err := strconv.Atoi(environment(name, strconv.Itoa(fallback)))
	if err != nil || value < 0 || value > maximum {
		return 0, fmt.Errorf("%s must be between 0 and %d", name, maximum)
	}
	return value, nil
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
