package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rubenvp8510/tempo-query-generator/internal/config"
	"github.com/rubenvp8510/tempo-query-generator/internal/generator"
)

func main() {
	flag.Parse()

	// Load and validate configuration
	cfg, err := config.LoadAndValidate()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Initialize metrics with the configured namespace
	metrics := generator.NewMetrics(cfg.Namespace)

	// Parse query delay (already validated in config package)
	queryDelay, err := time.ParseDuration(cfg.Query.Delay)
	if err != nil {
		slog.Error("could not parse query delay", "error", err)
		os.Exit(1)
	}

	totalConcurrency := cfg.Query.TotalConcurrency
	slog.Info("total concurrent workers", "count", totalConcurrency)

	// Apply QPS multiplier if configured (for compensation)
	targetQPS := cfg.Query.TargetQPS
	if cfg.Query.QPSMultiplier != 1.0 {
		targetQPS = targetQPS * cfg.Query.QPSMultiplier
		slog.Info("applied QPS multiplier", "multiplier", cfg.Query.QPSMultiplier, "adjusted_target", targetQPS)
	} else {
		slog.Info("target total QPS", "qps", targetQPS)
	}

	// Calculate per-worker QPS for fair distribution
	perWorkerQPS := targetQPS / float64(totalConcurrency)
	slog.Info("per-worker QPS", "qps", perWorkerQPS)

	slog.Info("rate limiter burst multiplier", "multiplier", cfg.Query.BurstMultiplier)
	slog.Info("query result limit", "limit", cfg.Query.Limit)

	// Set default click probability if not configured
	clickProbability := cfg.Query.ClickProbability
	if clickProbability == 0.0 {
		clickProbability = 0.5 // Default: 50% of searches will fetch full trace
	}
	slog.Info("trace fetch click probability", "probability", clickProbability)

	// Convert time buckets (already validated in config package)
	timeBuckets, err := config.ConvertTimeBuckets(cfg.TimeBuckets)
	if err != nil {
		slog.Error("failed to parse time buckets", "error", err)
		os.Exit(1)
	}
	slog.Info("using time buckets", "buckets", timeBuckets)

	slog.Info("loaded queries from configuration", "count", len(cfg.Queries))

	// Create query lookup map for fast access by name
	queriesMap := make(map[string]config.QueryDefinition)
	for _, q := range cfg.Queries {
		queriesMap[q.Name] = q
	}

	slog.Info("loaded execution plan from config", "entry_count", len(cfg.ExecutionPlan))

	// Count entries per query for logging
	queryDist := make(map[string]int)
	for _, entry := range cfg.ExecutionPlan {
		queryDist[entry.QueryName]++
	}

	slog.Info("plan distribution across queries")
	for queryName, count := range queryDist {
		slog.Info("query plan entries", "query", queryName, "entry_count", count, "note", "will cycle/repeat as needed")
	}

	// Create and start single executor for all queries
	executor := generator.NewExecutor(
		queriesMap,
		cfg.Namespace,
		cfg.Tempo.QueryEndpoint,
		cfg.TenantID,
		queryDelay,
		timeBuckets,
		totalConcurrency,
		targetQPS,
		cfg.Query.BurstMultiplier,
		cfg.Query.Limit,
		cfg.ExecutionPlan,
		metrics,
		clickProbability,
	)
	if err := executor.Run(); err != nil {
		slog.Error("could not run query executor", "error", err)
		os.Exit(1)
	}

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
