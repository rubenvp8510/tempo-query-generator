package generator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
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

// dynamicLimiter wraps rate.Limiter to allow dynamic limit updates
type dynamicLimiter struct {
	limiter *rate.Limiter
	mu      sync.RWMutex
}

// Wait waits for the limiter to allow an event
func (dl *dynamicLimiter) Wait(ctx context.Context) error {
	dl.mu.RLock()
	l := dl.limiter
	dl.mu.RUnlock()
	return l.Wait(ctx)
}

// SetLimit updates the limiter's rate limit
func (dl *dynamicLimiter) SetLimit(limit rate.Limit, burst int) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.limiter = rate.NewLimiter(limit, burst)
}

// Executor handles query execution with rate limiting and concurrency
type Executor struct {
	queries                 map[string]config.QueryDefinition
	namespace               string
	queryEndpoint           string
	delay                   time.Duration
	timeBuckets             []config.TimeBucket
	concurrency             int
	tenantID                string
	targetQPS               float64
	burstMultiplier         float64
	limit                   int
	executionPlan           []config.PlanEntry
	seed                    int64
	metrics                 *metrics.Metrics
	traceFetchProbability   float64
	bypassCache             bool
	jitter                  time.Duration
	rampUpDuration          time.Duration
	testDuration            time.Duration
	gracefulShutdownTimeout time.Duration
	ctx                     context.Context
	cancel                  context.CancelFunc
	workerLimiters          []*dynamicLimiter
	limiterMutex            sync.RWMutex
	currentTargetQPS        float64
	targetQPSMutex          sync.RWMutex
	done                    chan struct{} // Closed when graceful shutdown completes
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
	rampUpDuration time.Duration,
	testDuration time.Duration,
	gracefulShutdownTimeout time.Duration,
) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Executor{
		queries:                 queries,
		namespace:               namespace,
		queryEndpoint:           queryEndpoint,
		delay:                   delay,
		timeBuckets:             timeBuckets,
		concurrency:             concurrency,
		tenantID:                tenantID,
		targetQPS:               targetQPS,
		burstMultiplier:         burstMultiplier,
		limit:                   limit,
		executionPlan:           executionPlan,
		seed:                    seed,
		metrics:                 metrics,
		traceFetchProbability:   traceFetchProbability,
		bypassCache:             bypassCache,
		jitter:                  jitter,
		rampUpDuration:          rampUpDuration,
		testDuration:            testDuration,
		gracefulShutdownTimeout: gracefulShutdownTimeout,
		ctx:                     ctx,
		cancel:                  cancel,
		workerLimiters:          make([]*dynamicLimiter, 0, concurrency),
		done:                    make(chan struct{}),
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

	// Set test metadata in Prometheus
	e.metrics.TestMetadataGauge.WithLabelValues(
		testStartTime.Format(time.RFC3339),
		fmt.Sprintf("%.2f", e.targetQPS),
		fmt.Sprintf("%d", e.concurrency),
	).Set(1)

	// Calculate burst size for per-worker limiters
	burstSize := int(math.Max(10, perWorkerQPS*e.burstMultiplier))
	slog.Info("rate limiter configured", "per_worker_qps", perWorkerQPS, "burst", burstSize, "multiplier", e.burstMultiplier)

	// Initialize QPS to 0 if ramp-up is enabled, otherwise to target
	initialQPS := 0.0
	if e.rampUpDuration == 0 {
		initialQPS = e.targetQPS
	}
	e.metrics.TargetQPSGauge.Set(initialQPS)

	// Launch N independent workers for concurrent execution
	// Each worker uses seed + workerID for deterministic but unique RNG sequences
	for i := 0; i < e.concurrency; i++ {
		workerID := i + 1

		// Create per-worker rate limiter for fair distribution
		initialPerWorkerQPS := initialQPS / float64(e.concurrency)
		baseLimiter := rate.NewLimiter(rate.Limit(initialPerWorkerQPS), burstSize)
		limiter := &dynamicLimiter{limiter: baseLimiter}

		// Store limiter reference for ramp-up updates
		e.limiterMutex.Lock()
		e.workerLimiters = append(e.workerLimiters, limiter)
		e.limiterMutex.Unlock()

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
		go w.Run(e.ctx, e.delay)
	}

	// Launch test orchestration goroutine
	go e.orchestrateTestPhases()

	return nil
}

// Done returns a channel that is closed when the executor completes graceful shutdown
// For infinite tests (testDuration == 0), this channel will never be closed
func (e *Executor) Done() <-chan struct{} {
	return e.done
}

// orchestrateTestPhases manages the test lifecycle: ramp-up, steady-state, and shutdown
func (e *Executor) orchestrateTestPhases() {
	start := time.Now()

	// Phase 1: Ramp-up
	if e.rampUpDuration > 0 {
		e.metrics.TestPhaseGauge.Set(0)
		slog.Info("starting ramp-up phase", "duration", e.rampUpDuration)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		rampEnd := start.Add(e.rampUpDuration)

		for now := range ticker.C {
			if now.After(rampEnd) {
				break
			}
			elapsed := now.Sub(start)
			progress := float64(elapsed) / float64(e.rampUpDuration)
			if progress > 1.0 {
				progress = 1.0
			}
			currentTarget := e.targetQPS * progress

			// Update all worker rate limiters
			e.updateWorkerRateLimits(currentTarget)
			e.metrics.TargetQPSGauge.Set(currentTarget)
		}
		slog.Info("ramp-up phase complete", "final_qps", e.targetQPS)
	}

	// Phase 2: Steady-state
	e.metrics.TestPhaseGauge.Set(1)
	e.metrics.TargetQPSGauge.Set(e.targetQPS)
	remainingDuration := e.testDuration - e.rampUpDuration

	if remainingDuration > 0 {
		slog.Info("starting steady-state phase", "duration", remainingDuration)
		time.Sleep(remainingDuration)
		slog.Info("steady-state phase complete")
	} else if e.testDuration == 0 {
		// Infinite test, just return (done channel will never be closed)
		slog.Info("test running indefinitely", "note", "use SIGINT/SIGTERM to stop")
		return
	}

	// Phase 3: Graceful shutdown
	e.initiateGracefulShutdown()
}

// updateWorkerRateLimits updates all worker rate limiters to the new target QPS
func (e *Executor) updateWorkerRateLimits(newTargetQPS float64) {
	perWorkerQPS := newTargetQPS / float64(e.concurrency)
	burstSize := int(math.Max(10, perWorkerQPS*e.burstMultiplier))

	e.targetQPSMutex.Lock()
	e.currentTargetQPS = newTargetQPS
	e.targetQPSMutex.Unlock()

	e.limiterMutex.RLock()
	defer e.limiterMutex.RUnlock()

	for _, limiter := range e.workerLimiters {
		limiter.SetLimit(rate.Limit(perWorkerQPS), burstSize)
	}
}

// initiateGracefulShutdown signals workers to stop and waits for completion
func (e *Executor) initiateGracefulShutdown() {
	e.metrics.TestPhaseGauge.Set(2)
	slog.Info("initiating graceful shutdown", "timeout", e.gracefulShutdownTimeout)

	// Signal all workers to stop
	e.cancel()

	// Wait for shutdown timeout - workers will finish their current queries
	// The active workers gauge will naturally decrease as workers complete
	time.Sleep(e.gracefulShutdownTimeout)

	// Log final statistics
	// Note: Total queries count is available via Prometheus metrics at /metrics endpoint
	slog.Info("graceful shutdown complete", "note", "check Prometheus metrics for total queries executed")

	// Signal completion by closing the done channel
	close(e.done)
}
