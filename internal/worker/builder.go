package worker

import (
	"context"
	"math/rand"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
	"github.com/rubenvp8510/tempo-query-generator/internal/metrics"
)

// WorkerBuilder builds Worker instances using the builder pattern
type WorkerBuilder struct {
	workerID              int
	tempoClient           *client.TempoClient
	limiter               *rate.Limiter
	queries               map[string]config.QueryDefinition
	executionPlan         []config.PlanEntry
	timeBuckets           []config.TimeBucket
	metrics               *metrics.Metrics
	limit                 int
	testStartTime         time.Time
	seed                  int64
	traceFetchProbability float64
	jitter                time.Duration
}

// NewWorkerBuilder creates a new WorkerBuilder instance
func NewWorkerBuilder() *WorkerBuilder {
	return &WorkerBuilder{}
}

// WithWorkerID sets the worker ID
func (b *WorkerBuilder) WithWorkerID(workerID int) *WorkerBuilder {
	b.workerID = workerID
	return b
}

// WithTempoClient sets the Tempo client
func (b *WorkerBuilder) WithTempoClient(tempoClient *client.TempoClient) *WorkerBuilder {
	b.tempoClient = tempoClient
	return b
}

// WithLimiter sets the rate limiter
func (b *WorkerBuilder) WithLimiter(limiter *rate.Limiter) *WorkerBuilder {
	b.limiter = limiter
	return b
}

// WithQueries sets the queries map
func (b *WorkerBuilder) WithQueries(queries map[string]config.QueryDefinition) *WorkerBuilder {
	b.queries = queries
	return b
}

// WithTimeBuckets sets the time buckets
func (b *WorkerBuilder) WithTimeBuckets(timeBuckets []config.TimeBucket) *WorkerBuilder {
	b.timeBuckets = timeBuckets
	return b
}

// WithExecutionPlan sets the execution plan
func (b *WorkerBuilder) WithExecutionPlan(executionPlan []config.PlanEntry) *WorkerBuilder {
	b.executionPlan = executionPlan
	return b
}

// WithMetrics sets the metrics collector
func (b *WorkerBuilder) WithMetrics(metrics *metrics.Metrics) *WorkerBuilder {
	b.metrics = metrics
	return b
}

// WithLimit sets the query result limit
func (b *WorkerBuilder) WithLimit(limit int) *WorkerBuilder {
	b.limit = limit
	return b
}

// WithTestStartTime sets the test start time
func (b *WorkerBuilder) WithTestStartTime(testStartTime time.Time) *WorkerBuilder {
	b.testStartTime = testStartTime
	return b
}

// WithSeed sets the base seed for deterministic random number generation
func (b *WorkerBuilder) WithSeed(seed int64) *WorkerBuilder {
	b.seed = seed
	return b
}

// WithTraceFetchProbability sets the probability of fetching full trace after search
func (b *WorkerBuilder) WithTraceFetchProbability(traceFetchProbability float64) *WorkerBuilder {
	b.traceFetchProbability = traceFetchProbability
	return b
}

// WithJitter sets the maximum jitter duration to apply to time windows
func (b *WorkerBuilder) WithJitter(jitter time.Duration) *WorkerBuilder {
	b.jitter = jitter
	return b
}

// Build creates a Worker instance from the builder
func (b *WorkerBuilder) Build() *Worker {
	// Initialize deterministic RNG with seed + workerID (ensures each worker has unique but deterministic sequence)
	workerSeed := b.seed + int64(b.workerID)
	rng := rand.New(rand.NewSource(workerSeed))

	// Create bucket weight map for quick lookup during weighted selection
	bucketWeightMap := make(map[string]int)
	for _, bucket := range b.timeBuckets {
		weight := bucket.Weight
		if weight <= 0 {
			weight = 1 // Default weight
		}
		bucketWeightMap[bucket.Name] = weight
	}

	return &Worker{
		workerID:              b.workerID,
		tempoClient:           b.tempoClient,
		limiter:               b.limiter,
		queries:               b.queries,
		executionPlan:         b.executionPlan,
		timeBuckets:           b.timeBuckets,
		bucketWeightMap:       bucketWeightMap,
		metrics:               b.metrics,
		limit:                 b.limit,
		testStartTime:         b.testStartTime,
		ctx:                   context.Background(),
		rng:                   rng,
		traceFetchProbability: b.traceFetchProbability,
		jitter:                b.jitter,
	}
}
