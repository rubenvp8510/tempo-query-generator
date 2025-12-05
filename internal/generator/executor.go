package generator

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
	"github.com/rubenvp8510/tempo-query-generator/internal/metrics"
	"github.com/rubenvp8510/tempo-query-generator/internal/worker"
)

const (
	// kubernetesServiceAccountTokenPath is the default path for Kubernetes service account tokens
	kubernetesServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// Executor handles query execution with rate limiting and concurrency
type Executor struct {
	queries               map[string]config.QueryDefinition
	namespace             string
	queryEndpoint         string
	delay                 time.Duration
	timeBuckets           []config.TimeBucket
	concurrency           int
	tenantID              string
	targetQPS             float64
	burstMultiplier       float64
	limit                 int
	executionPlan         []config.PlanEntry
	seed                  int64
	metrics               *metrics.Metrics
	traceFetchProbability float64
	bypassCache           bool
	jitter                time.Duration
}

// createTempoClient creates a Tempo client with token fallback logic
func createTempoClient(queryEndpoint, tenantID, tokenPath string) (*client.TempoClient, error) {
	// Try creating client with token first
	tempoClient, err := client.NewTempoClient(queryEndpoint, tenantID, tokenPath)
	if err != nil {
		slog.Warn("failed to create Tempo client", "error", err)
		// Try creating client without token
		tempoClient, err = client.NewTempoClient(queryEndpoint, tenantID, "")
		if err != nil {
			return nil, fmt.Errorf("failed to create Tempo client: %w", err)
		}
	} else {
		slog.Info("service account token loaded")
	}
	return tempoClient, nil
}

// NewExecutor creates a new query executor that handles multiple queries
func NewExecutor(
	queries map[string]config.QueryDefinition,
	namespace, queryEndpoint, tenantID string,
	delay time.Duration,
	timeBuckets []config.TimeBucket,
	concurrency int,
	targetQPS, burstMultiplier float64,
	limit int,
	executionPlan []config.PlanEntry,
	seed int64,
	metrics *metrics.Metrics,
	traceFetchProbability float64,
	bypassCache bool,
	jitter time.Duration,
) *Executor {
	return &Executor{
		queries:               queries,
		namespace:             namespace,
		queryEndpoint:         queryEndpoint,
		delay:                 delay,
		timeBuckets:           timeBuckets,
		concurrency:           concurrency,
		tenantID:              tenantID,
		targetQPS:             targetQPS,
		burstMultiplier:       burstMultiplier,
		limit:                 limit,
		executionPlan:         executionPlan,
		seed:                  seed,
		metrics:               metrics,
		traceFetchProbability: traceFetchProbability,
		bypassCache:           bypassCache,
		jitter:                jitter,
	}
}

// Run starts the query executor with concurrent workers
func (e *Executor) Run() error {
	// Create Tempo client
	tempoClient, err := createTempoClient(e.queryEndpoint, e.tenantID, kubernetesServiceAccountTokenPath)
	if err != nil {
		return err
	}

	// Configure cache bypass if enabled
	if e.bypassCache {
		tempoClient.SetBypassCache(true)
		slog.Info("cache bypass enabled", "note", "Cache-Control headers will be sent with requests")
	}

	// Calculate per-worker QPS for fair distribution
	perWorkerQPS := e.targetQPS / float64(e.concurrency)
	slog.Info("starting query executor", "total_concurrency", e.concurrency, "target_qps", e.targetQPS, "per_worker_qps", perWorkerQPS)

	// Track when this executor started for time-aware bucket selection
	testStartTime := time.Now()

	// Calculate burst size for per-worker limiters
	burstSize := int(math.Max(10, perWorkerQPS*e.burstMultiplier))
	slog.Info("rate limiter configured", "per_worker_qps", perWorkerQPS, "burst", burstSize, "multiplier", e.burstMultiplier)

	// Launch N independent workers for concurrent execution
	// Each worker uses seed + workerID for deterministic but unique RNG sequences
	for i := 0; i < e.concurrency; i++ {
		workerID := i + 1

		// Create per-worker rate limiter for fair distribution
		limiter := rate.NewLimiter(rate.Limit(perWorkerQPS), burstSize)

		// Create and start worker using builder pattern
		w := worker.NewWorkerBuilder().
			WithWorkerID(workerID).
			WithTempoClient(tempoClient).
			WithLimiter(limiter).
			WithQueries(e.queries).
			WithExecutionPlan(e.executionPlan).
			WithTimeBuckets(e.timeBuckets).
			WithMetrics(e.metrics).
			WithLimit(e.limit).
			WithTestStartTime(testStartTime).
			WithSeed(e.seed).
			WithTraceFetchProbability(e.traceFetchProbability).
			WithJitter(e.jitter).
			Build()

		slog.Debug("worker created", "worker_id", workerID, "seed", e.seed)
		// Initial delay is handled deterministically inside the worker
		go w.Run(e.delay)
	}

	return nil
}
