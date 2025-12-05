package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/rubenvp8510/tempo-query-generator/internal/config"
)

func main() {
	// Load and validate configuration
	cfg, err := config.LoadAndValidate()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Determine Effective Target QPS (including multiplier)
	targetQPS := cfg.Query.TargetQPS
	if cfg.Query.QPSMultiplier > 0 && cfg.Query.QPSMultiplier != 1.0 {
		targetQPS = targetQPS * cfg.Query.QPSMultiplier
		slog.Info("applied QPS multiplier", "base_qps", cfg.Query.TargetQPS, "multiplier", cfg.Query.QPSMultiplier, "effective_target_qps", targetQPS)
	}

	// Get Concurrency
	concurrency := cfg.Query.TotalConcurrency

	// Get Trace Fetch Probability (using default logic from main app)
	traceFetchProb := cfg.Query.TraceFetchProbability
	if traceFetchProb == 0.0 {
		traceFetchProb = 0.5 // Default to 50% if not specified
	}

	// Log Configuration Summary
	slog.Info("capacity configuration analysis")
	slog.Info("configuration summary", "total_concurrency", concurrency, "target_qps", targetQPS, "trace_fetch_probability", traceFetchProb)

	if targetQPS <= 0 {
		slog.Error("target QPS must be positive", "target_qps", targetQPS)
		os.Exit(1)
	}

	// Calculate Theoretical Limits
	// Formula: Max Sustainable QPS = Concurrency / Average Latency
	// Therefore: Max Allowable Latency = Concurrency / Target QPS
	maxLatencySeconds := float64(concurrency) / targetQPS
	maxLatency := time.Duration(maxLatencySeconds * float64(time.Second))

	slog.Info("required performance", "target_qps", targetQPS, "concurrency", concurrency, "max_allowable_latency", maxLatency, "max_allowable_latency_ms", maxLatency.Milliseconds())
	slog.Info("note: iteration latency includes search latency plus trace fetch probability times trace fetch latency plus processing overhead")

	// Scenario Analysis
	slog.Info("scenario analysis: checking if configuration can sustain target QPS under various latency conditions")

	// Define various latency scenarios to check against
	scenarios := []struct {
		search time.Duration
		fetch  time.Duration
	}{
		{10 * time.Millisecond, 10 * time.Millisecond},
		{50 * time.Millisecond, 20 * time.Millisecond},
		{100 * time.Millisecond, 50 * time.Millisecond},
		{200 * time.Millisecond, 100 * time.Millisecond},
		{500 * time.Millisecond, 200 * time.Millisecond},
		{1 * time.Second, 500 * time.Millisecond},
		{maxLatency, maxLatency}, // The exact limit
	}

	for _, s := range scenarios {
		// Effective Latency = Search + (Prob * Fetch)
		effSeconds := s.search.Seconds() + (traceFetchProb * s.fetch.Seconds())
		effDuration := time.Duration(effSeconds * float64(time.Second))

		// Max QPS this latency allows
		maxPossibleQPS := float64(concurrency) / effSeconds

		// Determine status and log level
		if maxPossibleQPS < targetQPS {
			slog.Error("scenario fails: insufficient capacity",
				"avg_search_latency", s.search,
				"avg_fetch_latency", s.fetch,
				"effective_latency", effDuration,
				"max_possible_qps", maxPossibleQPS,
				"target_qps", targetQPS,
				"status", "BOTTLENECK")
		} else if maxPossibleQPS < targetQPS*1.1 {
			slog.Warn("scenario risky: near capacity limit",
				"avg_search_latency", s.search,
				"avg_fetch_latency", s.fetch,
				"effective_latency", effDuration,
				"max_possible_qps", maxPossibleQPS,
				"target_qps", targetQPS,
				"status", "NEAR_LIMIT")
		} else {
			slog.Info("scenario passes: sufficient capacity",
				"avg_search_latency", s.search,
				"avg_fetch_latency", s.fetch,
				"effective_latency", effDuration,
				"max_possible_qps", maxPossibleQPS,
				"target_qps", targetQPS,
				"status", "OK")
		}
	}

	slog.Info("capacity analysis complete")
}

