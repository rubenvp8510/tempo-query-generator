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
	"github.com/rubenvp8510/tempo-query-generator/internal/metrics"
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
	m := metrics.NewMetrics(cfg.Namespace)

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

	// Set default trace fetch probability if not configured
	traceFetchProbability := cfg.Query.TraceFetchProbability
	if traceFetchProbability == 0.0 {
		traceFetchProbability = 0.5 // Default: 50% of searches will fetch full trace
	}
	slog.Info("trace fetch probability", "probability", traceFetchProbability)

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

	// Get seed for deterministic random number generation
	seed := cfg.Query.Seed
	slog.Info("using seed for deterministic load generation", "seed", seed, "note", "each worker uses seed + workerID")

	// Parse time window jitter (optional, defaults to 0 if not set)
	var jitter time.Duration
	if cfg.Query.TimeWindowJitter != "" {
		var err error
		jitter, err = time.ParseDuration(cfg.Query.TimeWindowJitter)
		if err != nil {
			slog.Error("could not parse time window jitter", "error", err)
			os.Exit(1)
		}
		slog.Info("time window jitter enabled", "jitter", jitter, "note", "random time shifts will be applied to query windows to defeat caching")
	} else {
		slog.Info("time window jitter disabled", "note", "query windows will use fixed boundaries")
	}

	// Get cache bypass setting
	bypassCache := cfg.Query.BypassCache
	if bypassCache {
		slog.Info("cache bypass enabled", "note", "Cache-Control headers will be sent with requests")
	}

	slog.Info("loaded execution plan from config", "entry_count", len(cfg.ExecutionPlan))

	// Count entries per query for logging
	queryDist := make(map[string]int)
	for _, entry := range cfg.ExecutionPlan {
		queryDist[entry.QueryName]++
	}

	slog.Info("plan distribution across queries")
	for queryName, count := range queryDist {
		slog.Info("query plan entries", "query", queryName, "entry_count", count, "note", "weighted random selection based on bucket weights")
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
		seed,
		m,
		traceFetchProbability,
		bypassCache,
		jitter,
	)
	if err := executor.Run(); err != nil {
		slog.Error("could not run query executor", "error", err)
		os.Exit(1)
	}

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
