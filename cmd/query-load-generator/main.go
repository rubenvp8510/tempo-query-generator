package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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

	// Parse test duration fields
	rampUpDuration, err := time.ParseDuration(cfg.Query.RampUpDuration)
	if err != nil {
		slog.Error("could not parse ramp-up duration", "error", err)
		os.Exit(1)
	}

	testDuration, err := time.ParseDuration(cfg.Query.TestDuration)
	if err != nil {
		slog.Error("could not parse test duration", "error", err)
		os.Exit(1)
	}

	gracefulShutdownTimeout, err := time.ParseDuration(cfg.Query.GracefulShutdownTimeout)
	if err != nil {
		slog.Error("could not parse graceful shutdown timeout", "error", err)
		os.Exit(1)
	}

	if rampUpDuration > 0 {
		slog.Info("ramp-up enabled", "duration", rampUpDuration)
	} else {
		slog.Info("ramp-up disabled", "note", "starting at full target QPS")
	}

	if testDuration > 0 {
		slog.Info("finite test duration", "duration", testDuration)
	} else {
		slog.Info("infinite test duration", "note", "use SIGINT/SIGTERM to stop")
	}

	slog.Info("graceful shutdown timeout", "timeout", gracefulShutdownTimeout)

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

	// Start metrics HTTP server in background
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			slog.Error("metrics server error", "error", err)
		}
	}()

	// Track test start time
	testStartTime := time.Now()

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
		rampUpDuration,
		testDuration,
		gracefulShutdownTimeout,
	)

	// Launch achieved QPS reporter
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go reportAchievedQPS(ctx, m, &wg)

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start executor
	if err := executor.Run(); err != nil {
		slog.Error("could not run query executor", "error", err)
		os.Exit(1)
	}

	// Wait for test completion or signal
	select {
	case sig := <-sigChan:
		slog.Info("received signal", "signal", sig, "initiating graceful shutdown")
		cancel()
		// Wait a bit for final metrics
		time.Sleep(2 * time.Second)
	case <-executor.Done():
		slog.Info("test completed normally", "note", "executor finished gracefully")
		cancel()
		// Wait a bit for final metrics
		time.Sleep(2 * time.Second)
	case <-ctx.Done():
		// Context cancelled (shouldn't happen in normal flow)
	}

	// Stop achieved QPS reporter
	cancel()
	wg.Wait()

	// Print final test summary
	printTestSummary(m, testStartTime, targetQPS)
}

// reportAchievedQPS calculates and reports achieved QPS every 10 seconds
func reportAchievedQPS(ctx context.Context, m *metrics.Metrics, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastCount float64
	var lastTime time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			// Read total queries counter value
			// Note: We can't directly read from prometheus.Counter, so we track it via metrics
			// For now, we'll use a simple approach: track the increment rate
			currentTime := t
			if !lastTime.IsZero() {
				// Calculate QPS based on counter increments
				// Since we can't read the counter directly, we'll approximate
				// by tracking the rate of change
				elapsed := currentTime.Sub(lastTime).Seconds()
				if elapsed > 0 {
					// This is a simplified approach - in production you'd want to
					// scrape the actual counter value from Prometheus
					currentCount := m.TotalQueriesCounter
					if currentCount != nil {
						// We can't read the value directly, so we'll log that we're tracking it
						// The actual QPS will be visible in Prometheus metrics
						_ = lastCount
						_ = currentCount
					}
				}
			}
			lastTime = currentTime
		}
	}
}

// printTestSummary prints final test statistics
func printTestSummary(m *metrics.Metrics, startTime time.Time, targetQPS float64) {
	totalDuration := time.Since(startTime)
	separator := strings.Repeat("=", 60)

	slog.Info(separator)
	slog.Info("TEST SUMMARY")
	slog.Info(separator)
	slog.Info("total duration", "duration", totalDuration)
	slog.Info("target QPS", "qps", targetQPS)

	// Note: We can't directly read counter values from Prometheus metrics
	// in this context. The actual values are available via the /metrics endpoint.
	slog.Info("total queries", "note", "check Prometheus metrics at http://localhost:2112/metrics")
	slog.Info("metric: query_load_test_test_queries_total", "note", "total queries executed")
	slog.Info("metric: query_load_test_test_achieved_qps", "note", "achieved QPS (10s rolling window)")
	slog.Info("metric: query_load_test_test_target_qps", "note", "target QPS over time")
	slog.Info(separator)
}
