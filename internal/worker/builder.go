package worker

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
	"github.com/rubenvp8510/tempo-query-generator/internal/metrics"
)

// WorkerBuilder builds Worker instances using the builder pattern
type WorkerBuilder struct {
	workerID         int
	tempoClient      *client.TempoClient
	limiter          *rate.Limiter
	queries          map[string]config.QueryDefinition
	timeBuckets      []config.TimeBucket
	executionPlan     []config.PlanEntry
	metrics          *metrics.Metrics
	limit            int
	testStartTime    time.Time
	initialPlanIndex     int64
	traceFetchProbability float64
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

// WithInitialPlanIndex sets the initial plan index for staggered start positions
func (b *WorkerBuilder) WithInitialPlanIndex(initialPlanIndex int64) *WorkerBuilder {
	b.initialPlanIndex = initialPlanIndex
	return b
}

// WithTraceFetchProbability sets the probability of fetching full trace after search
func (b *WorkerBuilder) WithTraceFetchProbability(traceFetchProbability float64) *WorkerBuilder {
	b.traceFetchProbability = traceFetchProbability
	return b
}

// Build creates a Worker instance from the builder
func (b *WorkerBuilder) Build() *Worker {
	return &Worker{
		workerID:         b.workerID,
		tempoClient:      b.tempoClient,
		limiter:          b.limiter,
		queries:          b.queries,
		timeBuckets:      b.timeBuckets,
		executionPlan:    b.executionPlan,
		metrics:          b.metrics,
		limit:            b.limit,
		testStartTime:    b.testStartTime,
		ctx:                  context.Background(),
		planIndex:            b.initialPlanIndex, // Initialize with staggered offset
		traceFetchProbability: b.traceFetchProbability,
	}
}

