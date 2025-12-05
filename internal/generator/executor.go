package generator

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand"
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
	metrics               *metrics.Metrics
	traceFetchProbability float64
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
	metrics *metrics.Metrics,
	traceFetchProbability float64,
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
		metrics:               metrics,
		traceFetchProbability: traceFetchProbability,
	}
}

// Run starts the query executor with concurrent workers
func (e *Executor) Run() error {
	// Create Tempo client
	tempoClient, err := createTempoClient(e.queryEndpoint, e.tenantID, kubernetesServiceAccountTokenPath)
	if err != nil {
		return err
	}

	// Calculate per-worker QPS for fair distribution
	perWorkerQPS := e.targetQPS / float64(e.concurrency)
	slog.Info("starting query executor", "total_concurrency", e.concurrency, "target_qps", e.targetQPS, "per_worker_qps", perWorkerQPS)

	// Track when this executor started for time-aware bucket selection
	testStartTime := time.Now()

	// Calculate burst size for per-worker limiters
	burstSize := int(math.Max(10, perWorkerQPS*e.burstMultiplier))
	slog.Info("rate limiter configured", "per_worker_qps", perWorkerQPS, "burst", burstSize, "multiplier", e.burstMultiplier)

	// Launch N independent workers for concurrent execution with staggered starting positions
	planLength := int64(len(e.executionPlan))
	for i := 0; i < e.concurrency; i++ {
		workerID := i + 1
		// Each worker starts with a small random initial delay to spread the load
		initialDelay := time.Duration(rand.Int63n(int64(time.Second)))

		// Calculate staggered starting position in execution plan for better distribution
		// This ensures workers don't all start at the same position, reducing cache effects
		initialPlanIndex := (int64(i) * planLength) / int64(e.concurrency)

		// Create per-worker rate limiter for fair distribution
		limiter := rate.NewLimiter(rate.Limit(perWorkerQPS), burstSize)

		// Create and start worker using builder pattern
		w := worker.NewWorkerBuilder().
			WithWorkerID(workerID).
			WithTempoClient(tempoClient).
			WithLimiter(limiter).
			WithQueries(e.queries).
			WithTimeBuckets(e.timeBuckets).
			WithExecutionPlan(e.executionPlan).
			WithMetrics(e.metrics).
			WithLimit(e.limit).
			WithTestStartTime(testStartTime).
			WithInitialPlanIndex(initialPlanIndex).
			WithTraceFetchProbability(e.traceFetchProbability).
			Build()

		slog.Debug("worker starting position", "worker_id", workerID, "initial_plan_index", initialPlanIndex)
		go w.Run(initialDelay)
	}

	return nil
}
