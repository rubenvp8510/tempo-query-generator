package main

import (
	"flag"
	"log"
	"net/http"
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
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize metrics with the configured namespace
	metrics := generator.NewMetrics(cfg.Namespace)

	// Parse query delay (already validated in config package)
	queryDelay, err := time.ParseDuration(cfg.Query.Delay)
	if err != nil {
		log.Fatalf("Could not parse query delay: %v", err)
	}

	concurrentQueries := cfg.Query.ConcurrentQueries
	log.Printf("Concurrent queries per executor: %d", concurrentQueries)

	// Apply QPS multiplier if configured (for compensation)
	targetQPS := cfg.Query.TargetQPS
	if cfg.Query.QPSMultiplier != 1.0 {
		targetQPS = targetQPS * cfg.Query.QPSMultiplier
		log.Printf("Applied QPS multiplier: %.2f (adjusted target: %.2f)", cfg.Query.QPSMultiplier, targetQPS)
	} else {
		log.Printf("Target total QPS: %.2f", targetQPS)
	}

	log.Printf("Rate limiter burst multiplier: %.2f", cfg.Query.BurstMultiplier)
	log.Printf("Query result limit: %d", cfg.Query.Limit)

	// Convert time buckets (already validated in config package)
	timeBuckets, err := config.ConvertTimeBuckets(cfg.TimeBuckets)
	if err != nil {
		log.Fatalf("Failed to parse time buckets: %v", err)
	}
	log.Printf("Using time buckets: %+v", timeBuckets)

	log.Printf("Loaded %d queries from configuration", len(cfg.Queries))

	// Calculate per-query QPS: total QPS divided by number of query types
	perQueryQPS := targetQPS / float64(len(cfg.Queries))
	log.Printf("Per-query QPS: %.4f (distributed across %d concurrent workers)", perQueryQPS, concurrentQueries)

	log.Printf("Loaded execution plan with %d entries from config", len(cfg.ExecutionPlan))

	// Count entries per query for logging
	queryDist := make(map[string]int)
	for _, entry := range cfg.ExecutionPlan {
		queryDist[entry.QueryName]++
	}

	log.Printf("Plan distribution across queries:")
	for queryName, count := range queryDist {
		log.Printf("  %s: %d entries (will cycle/repeat as needed)", queryName, count)
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
			log.Fatalf("Could not run query executor: %v", err)
		}
	}

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
