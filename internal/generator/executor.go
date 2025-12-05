package generator

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
)

const (
	// kubernetesServiceAccountTokenPath is the default path for Kubernetes service account tokens
	kubernetesServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// Executor handles query execution with rate limiting and concurrency
type Executor struct {
	name            string
	namespace       string
	queryEndpoint   string
	traceQL         string
	delay           time.Duration
	timeBuckets     []config.TimeBucket
	concurrency     int
	tenantID        string
	targetQPS       float64
	burstMultiplier float64
	limit           int
	executionPlan   []config.PlanEntry
	metrics         *Metrics
}

// planIndices stores atomic counters for each query name to cycle through plan entries
var planIndices = make(map[string]*int64)
var planIndicesMutex sync.Mutex

// getPlanIndex returns the atomic counter for a given query name, creating it if needed
func getPlanIndex(queryName string) *int64 {
	planIndicesMutex.Lock()
	defer planIndicesMutex.Unlock()

	if idx, exists := planIndices[queryName]; exists {
		return idx
	}

	idx := new(int64)
	planIndices[queryName] = idx
	return idx
}

// createTempoClient creates a Tempo client with token fallback logic
func createTempoClient(queryEndpoint, tenantID, tokenPath string) (*client.TempoClient, error) {
	// Try creating client with token first
	tempoClient, err := client.NewTempoClient(queryEndpoint, tenantID, tokenPath)
	if err != nil {
		log.Printf("Warning: Failed to create Tempo client: %v", err)
		// Try creating client without token
		tempoClient, err = client.NewTempoClient(queryEndpoint, tenantID, "")
		if err != nil {
			return nil, fmt.Errorf("failed to create Tempo client: %w", err)
		}
	} else {
		log.Printf("ServiceAccount Token loaded")
	}
	return tempoClient, nil
}

// NewExecutor creates a new query executor
func NewExecutor(
	name, namespace, queryEndpoint, traceQL, tenantID string,
	delay time.Duration,
	timeBuckets []config.TimeBucket,
	concurrency int,
	targetQPS, burstMultiplier float64,
	limit int,
	executionPlan []config.PlanEntry,
	metrics *Metrics,
) *Executor {
	return &Executor{
		name:            name,
		namespace:       namespace,
		queryEndpoint:   queryEndpoint,
		traceQL:         traceQL,
		delay:           delay,
		timeBuckets:     timeBuckets,
		concurrency:     concurrency,
		tenantID:        tenantID,
		targetQPS:       targetQPS,
		burstMultiplier: burstMultiplier,
		limit:           limit,
		executionPlan:   executionPlan,
		metrics:         metrics,
	}
}

// Run starts the query executor with concurrent workers
func (e *Executor) Run() error {
	// Create Tempo client
	tempoClient, err := createTempoClient(e.queryEndpoint, e.tenantID, kubernetesServiceAccountTokenPath)
	if err != nil {
		return err
	}

	log.Printf("Starting query executor for: %s (concurrency: %d, target QPS: %.4f)\n", e.name, e.concurrency, e.targetQPS)

	// Track when this executor started for time-aware bucket selection
	testStartTime := time.Now()

	// Create a shared rate limiter for all workers of this query type
	// The limiter ensures total QPS for this query type equals targetQPS
	// Calculate burst size: allow 1-2 seconds of burst capacity for better rate accuracy
	burstSize := int(math.Max(10, e.targetQPS*e.burstMultiplier))
	limiter := rate.NewLimiter(rate.Limit(e.targetQPS), burstSize)
	log.Printf("Rate limiter for %s: QPS=%.4f, burst=%d (multiplier=%.2f)", e.name, e.targetQPS, burstSize, e.burstMultiplier)

	// Launch N independent workers for concurrent execution
	for i := 0; i < e.concurrency; i++ {
		workerID := i + 1
		// Each worker starts with a small random initial delay to spread the load
		initialDelay := time.Duration(rand.Int63n(int64(time.Second)))

		// Create and start worker using builder pattern
		worker := NewWorkerBuilder().
			WithWorkerID(workerID).
			WithTempoClient(tempoClient).
			WithLimiter(limiter).
			WithName(e.name).
			WithTraceQL(e.traceQL).
			WithTimeBuckets(e.timeBuckets).
			WithExecutionPlan(e.executionPlan).
			WithMetrics(e.metrics).
			WithLimit(e.limit).
			WithTestStartTime(testStartTime).
			Build()

		go worker.Run(initialDelay)
	}

	return nil
}
