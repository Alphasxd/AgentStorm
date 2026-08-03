package main

import "testing"

func configureRequiredEnvironment(t *testing.T) {
	t.Helper()
	for name, value := range map[string]string{
		"AGENTSTORM_DATABASE_URL":               "postgres://tests",
		"AGENTSTORM_S3_ENDPOINT":                "minio:9000",
		"AGENTSTORM_S3_ACCESS_KEY":              "access",
		"AGENTSTORM_S3_SECRET_KEY":              "secret",
		"AGENTSTORM_RESULT_WRITE_TOKEN":         "write",
		"AGENTSTORM_RESULT_READ_TOKEN":          "read",
		"AGENTSTORM_GLOBAL_MAX_CONCURRENCY":     "",
		"AGENTSTORM_GLOBAL_REQUESTS_PER_MINUTE": "",
		"AGENTSTORM_PROVIDER_LIMITS_JSON":       "",
		"AGENTSTORM_PERMIT_LEASE_SECONDS":       "",
	} {
		t.Setenv(name, value)
	}
}

func TestLoadConfigParsesDistributedLimitPolicy(t *testing.T) {
	configureRequiredEnvironment(t)
	t.Setenv("AGENTSTORM_GLOBAL_MAX_CONCURRENCY", "12")
	t.Setenv("AGENTSTORM_GLOBAL_REQUESTS_PER_MINUTE", "120")
	t.Setenv("AGENTSTORM_PROVIDER_LIMITS_JSON", `{"fake":{"max_concurrency":3,"requests_per_minute":30}}`)
	t.Setenv("AGENTSTORM_PERMIT_LEASE_SECONDS", "45")

	config, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.limitPolicy.Global.MaxConcurrency != 12 ||
		config.limitPolicy.Global.RequestsPerMinute != 120 ||
		config.limitPolicy.Providers["fake"].MaxConcurrency != 3 ||
		config.limitPolicy.LeaseDuration.Seconds() != 45 {
		t.Fatalf("unexpected limit policy: %#v", config.limitPolicy)
	}
}

func TestLoadConfigRejectsInvalidDistributedLimits(t *testing.T) {
	for name, value := range map[string]string{
		"global":   "-1",
		"provider": `{"fake":{"max_concurrency":-1}}`,
		"unknown":  `{"fake":{"max_concurrncy":1}}`,
		"lease":    "9",
	} {
		t.Run(name, func(t *testing.T) {
			configureRequiredEnvironment(t)
			switch name {
			case "global":
				t.Setenv("AGENTSTORM_GLOBAL_MAX_CONCURRENCY", value)
			case "provider", "unknown":
				t.Setenv("AGENTSTORM_PROVIDER_LIMITS_JSON", value)
			case "lease":
				t.Setenv("AGENTSTORM_PERMIT_LEASE_SECONDS", value)
			}
			if _, err := loadConfig(); err == nil {
				t.Fatal("invalid distributed limit configuration was accepted")
			}
		})
	}
}
