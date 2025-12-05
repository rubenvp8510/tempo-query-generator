package config

import "time"

// PlanEntry represents a single entry in the execution plan from config
type PlanEntry struct {
	QueryName  string `yaml:"queryName" mapstructure:"queryName" validate:"required"`
	BucketName string `yaml:"bucketName" mapstructure:"bucketName" validate:"required"`
}

// TempoConfig represents Tempo-specific configuration
type TempoConfig struct {
	QueryEndpoint string `yaml:"queryEndpoint" mapstructure:"queryEndpoint" validate:"required"`
}

// QueryConfig represents query execution configuration
type QueryConfig struct {
	Delay             string  `yaml:"delay" mapstructure:"delay" validate:"required"`
	ConcurrentQueries int     `yaml:"concurrentQueries" mapstructure:"concurrentQueries" validate:"required,gte=1"`
	TargetQPS         float64 `yaml:"targetQPS" mapstructure:"targetQPS" validate:"required,gt=0"`
	BurstMultiplier   float64 `yaml:"burstMultiplier" mapstructure:"burstMultiplier" validate:"omitempty,gt=0"` // Multiplier for rate limiter burst size (default: 2.0)
	QPSMultiplier     float64 `yaml:"qpsMultiplier" mapstructure:"qpsMultiplier" validate:"omitempty,gt=0"`     // Multiplier to apply to targetQPS for compensation (default: 1.0)
	Limit             int     `yaml:"limit" mapstructure:"limit" validate:"omitempty,gt=0"`                     // Maximum number of results to return per query (default: 1000)
}

// TimeBucketConfig represents a time bucket configuration from YAML
type TimeBucketConfig struct {
	Name     string `yaml:"name" mapstructure:"name" validate:"required"`
	AgeStart string `yaml:"ageStart" mapstructure:"ageStart" validate:"required"`
	AgeEnd   string `yaml:"ageEnd" mapstructure:"ageEnd" validate:"required"`
	Weight   int    `yaml:"weight" mapstructure:"weight" validate:"required,gt=0"`
}

// QueryDefinition represents a single query definition
type QueryDefinition struct {
	Name    string `yaml:"name" mapstructure:"name" validate:"required"`
	TraceQL string `yaml:"traceql" mapstructure:"traceql" validate:"required"`
}

// Config represents the YAML configuration structure
type Config struct {
	Tempo         TempoConfig        `yaml:"tempo" mapstructure:"tempo" validate:"required"`
	Namespace     string             `yaml:"namespace" mapstructure:"namespace" validate:"required"`
	TenantID      string             `yaml:"tenantId" mapstructure:"tenantId" validate:"required"`
	Query         QueryConfig        `yaml:"query" mapstructure:"query" validate:"required"`
	TimeBuckets   []TimeBucketConfig `yaml:"timeBuckets" mapstructure:"timeBuckets" validate:"required,dive"`
	Queries       []QueryDefinition  `yaml:"queries" mapstructure:"queries" validate:"required,min=1,dive"`
	ExecutionPlan []PlanEntry        `yaml:"executionPlan" mapstructure:"executionPlan" validate:"required,min=1,dive"` // Execution plan defined in config
}

// TimeBucket defines a time range for queries (runtime representation with parsed durations)
type TimeBucket struct {
	Name     string        // bucket name (e.g., "ingester", "backend-1h")
	AgeStart time.Duration // how far back to end the query window
	AgeEnd   time.Duration // how far back to start the query window
	Weight   int           // weight for random selection
}

