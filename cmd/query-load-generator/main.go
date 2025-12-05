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

	concurrentQueries := cfg.Query.ConcurrentQueries
	slog.Info("concurrent queries per executor", "count", concurrentQueries)

	// Apply QPS multiplier if configured (for compensation)
	targetQPS := cfg.Query.TargetQPS
	if cfg.Query.QPSMultiplier != 1.0 {
		targetQPS = targetQPS * cfg.Query.QPSMultiplier
		slog.Info("applied QPS multiplier", "multiplier", cfg.Query.QPSMultiplier, "adjusted_target", targetQPS)
	} else {
		slog.Info("target total QPS", "qps", targetQPS)
	}

	slog.Info("rate limiter burst multiplier", "multiplier", cfg.Query.BurstMultiplier)
	slog.Info("query result limit", "limit", cfg.Query.Limit)

	// Convert time buckets (already validated in config package)
	timeBuckets, err := config.ConvertTimeBuckets(cfg.TimeBuckets)
	if err != nil {
		slog.Error("failed to parse time buckets", "error", err)
		os.Exit(1)
	}
	slog.Info("using time buckets", "buckets", timeBuckets)

	slog.Info("loaded queries from configuration", "count", len(cfg.Queries))

	// Calculate per-query QPS: total QPS divided by number of query types
	perQueryQPS := targetQPS / float64(len(cfg.Queries))
	slog.Info("per-query QPS", "qps", perQueryQPS, "concurrent_workers", concurrentQueries)

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

	// Create and start query executors
	for _, q := range cfg.Queries {
		executor := generator.NewExecutor(
			q.Name,
			cfg.Namespace,
			cfg.Tempo.QueryEndpoint,
			q.TraceQL,
			cfg.TenantID,
			queryDelay,
			timeBuckets,
			concurrentQueries,
			perQueryQPS,
			cfg.Query.BurstMultiplier,
			cfg.Query.Limit,
			cfg.ExecutionPlan,
			metrics,
		)
		if err := executor.Run(); err != nil {
			slog.Error("could not run query executor", "error", err)
			os.Exit(1)
		}
	}

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
