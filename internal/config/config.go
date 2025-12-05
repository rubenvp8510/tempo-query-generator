package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

var (
	validate *validator.Validate
)

func init() {
	validate = validator.New()
}

// SetDefaults sets default values for optional fields
func SetDefaults(cfg *Config) {
	if cfg.Query.BurstMultiplier <= 0 {
		cfg.Query.BurstMultiplier = 2.0
	}
	if cfg.Query.QPSMultiplier <= 0 {
		cfg.Query.QPSMultiplier = 1.0
	}
	if cfg.Query.Limit <= 0 {
		cfg.Query.Limit = 1000
	}
	// Set default weights for time buckets if not specified
	for i := range cfg.TimeBuckets {
		if cfg.TimeBuckets[i].Weight <= 0 {
			cfg.TimeBuckets[i].Weight = 1
		}
	}
}

// LoadAndValidate loads configuration from YAML file and validates it
func LoadAndValidate() (*Config, error) {
	v := viper.New()

	// Get config file path from environment variable (default to /config/config.yaml)
	configPath := os.Getenv("TEMPO_CONFIG_FILE")
	if configPath == "" {
		configPath = os.Getenv("CONFIG_FILE")
		if configPath == "" {
			configPath = "/config/config.yaml"
		}
	}

	// Set config file path
	v.SetConfigFile(configPath)

	// Set config type
	v.SetConfigType("yaml")

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Unmarshal config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Set defaults for optional fields
	SetDefaults(&cfg)

	// Validate config
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// validateConfig validates the configuration struct
func validateConfig(cfg *Config) error {
	// Validate struct tags
	if err := validate.Struct(cfg); err != nil {
		return formatValidationError(err)
	}

	// Validate delay duration
	if _, err := time.ParseDuration(cfg.Query.Delay); err != nil {
		return fmt.Errorf("invalid query delay duration: %w", err)
	}

	// Validate time window jitter duration
	if cfg.Query.TimeWindowJitter != "" {
		if _, err := time.ParseDuration(cfg.Query.TimeWindowJitter); err != nil {
			return fmt.Errorf("invalid time window jitter duration: %w", err)
		}
	}

	// Validate time bucket durations
	for _, bucket := range cfg.TimeBuckets {
		if _, err := time.ParseDuration(bucket.AgeStart); err != nil {
			return fmt.Errorf("invalid ageStart duration in bucket %s: %w", bucket.Name, err)
		}
		if _, err := time.ParseDuration(bucket.AgeEnd); err != nil {
			return fmt.Errorf("invalid ageEnd duration in bucket %s: %w", bucket.Name, err)
		}
		// Set default weight if not specified
		if bucket.Weight <= 0 {
			// This will be handled in SetDefaults, but we validate here too
		}
	}

	// Validate execution plan consistency
	queryMap := make(map[string]bool)
	for _, q := range cfg.Queries {
		queryMap[q.Name] = true
	}

	bucketMap := make(map[string]bool)
	for _, bucket := range cfg.TimeBuckets {
		bucketMap[bucket.Name] = true
	}

	for _, entry := range cfg.ExecutionPlan {
		if !queryMap[entry.QueryName] {
			return fmt.Errorf("execution plan references undefined query: %s", entry.QueryName)
		}
		if !bucketMap[entry.BucketName] {
			return fmt.Errorf("execution plan references undefined bucket: %s", entry.BucketName)
		}
	}

	return nil
}

// formatValidationError formats validator errors into a readable string
func formatValidationError(err error) error {
	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		var errMsgs []string
		for _, e := range validationErrors {
			errMsgs = append(errMsgs, fmt.Sprintf("field '%s' failed validation: %s", e.Field(), getValidationErrorMsg(e)))
		}
		return fmt.Errorf("%s", strings.Join(errMsgs, "; "))
	}
	return err
}

// getValidationErrorMsg returns a human-readable error message for validation errors
func getValidationErrorMsg(e validator.FieldError) string {
	switch e.Tag() {
	case "required":
		return "field is required"
	case "gte":
		return fmt.Sprintf("must be greater than or equal to %s", e.Param())
	case "gt":
		return fmt.Sprintf("must be greater than %s", e.Param())
	case "min":
		return fmt.Sprintf("must have at least %s items", e.Param())
	default:
		return fmt.Sprintf("failed validation tag: %s", e.Tag())
	}
}

// ConvertTimeBuckets converts config time buckets to internal TimeBucket struct
func ConvertTimeBuckets(configBuckets []TimeBucketConfig) ([]TimeBucket, error) {
	buckets := make([]TimeBucket, 0, len(configBuckets))

	for _, cb := range configBuckets {
		ageStart, err := time.ParseDuration(cb.AgeStart)
		if err != nil {
			return nil, fmt.Errorf("invalid ageStart duration in bucket %s: %v", cb.Name, err)
		}

		ageEnd, err := time.ParseDuration(cb.AgeEnd)
		if err != nil {
			return nil, fmt.Errorf("invalid ageEnd duration in bucket %s: %v", cb.Name, err)
		}

		weight := cb.Weight
		if weight <= 0 {
			weight = 1 // Default weight
		}

		buckets = append(buckets, TimeBucket{
			Name:     cb.Name,
			AgeStart: ageStart,
			AgeEnd:   ageEnd,
			Weight:   weight,
		})
	}

	return buckets, nil
}
