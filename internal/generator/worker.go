package generator

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
)

// Worker handles individual query execution in a concurrent worker
type Worker struct {
	workerID      int
	tempoClient   *client.TempoClient
	limiter       *rate.Limiter
	name          string
	traceQL       string
	timeBuckets   []config.TimeBucket
	executionPlan []config.PlanEntry
	metrics       *Metrics
	limit         int
	testStartTime time.Time
	ctx           context.Context
}

// WorkerBuilder builds Worker instances using the builder pattern
type WorkerBuilder struct {
	workerID      int
	tempoClient   *client.TempoClient
	limiter       *rate.Limiter
	name          string
	traceQL       string
	timeBuckets   []config.TimeBucket
	executionPlan []config.PlanEntry
	metrics       *Metrics
	limit         int
	testStartTime time.Time
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

// WithName sets the query name
func (b *WorkerBuilder) WithName(name string) *WorkerBuilder {
	b.name = name
	return b
}

// WithTraceQL sets the TraceQL query string
func (b *WorkerBuilder) WithTraceQL(traceQL string) *WorkerBuilder {
	b.traceQL = traceQL
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
func (b *WorkerBuilder) WithMetrics(metrics *Metrics) *WorkerBuilder {
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

// Build creates a Worker instance from the builder
func (b *WorkerBuilder) Build() *Worker {
	return &Worker{
		workerID:      b.workerID,
		tempoClient:   b.tempoClient,
		limiter:       b.limiter,
		name:          b.name,
		traceQL:       b.traceQL,
		timeBuckets:   b.timeBuckets,
		executionPlan: b.executionPlan,
		metrics:       b.metrics,
		limit:         b.limit,
		testStartTime: b.testStartTime,
		ctx:           context.Background(),
	}
}

// NewWorker creates a new worker instance (deprecated: use WorkerBuilder instead)
func NewWorker(
	workerID int,
	tempoClient *client.TempoClient,
	limiter *rate.Limiter,
	name, traceQL string,
	timeBuckets []config.TimeBucket,
	executionPlan []config.PlanEntry,
	metrics *Metrics,
	limit int,
	testStartTime time.Time,
) *Worker {
	return &Worker{
		workerID:      workerID,
		tempoClient:   tempoClient,
		limiter:       limiter,
		name:          name,
		traceQL:       traceQL,
		timeBuckets:   timeBuckets,
		executionPlan: executionPlan,
		metrics:       metrics,
		limit:         limit,
		testStartTime: testStartTime,
		ctx:           context.Background(),
	}
}

// countSpansFromResponse counts the total number of spans in a Tempo search response
func countSpansFromResponse(searchResp *client.TempoSearchResponse) int {
	if searchResp == nil {
		return 0
	}

	var spansCount int
	// Count total spans across all traces (Tempo format)
	for _, trace := range searchResp.Traces {
		// Check SpanSets (for structural queries)
		for _, spanSet := range trace.SpanSets {
			spansCount += len(spanSet.Spans)
		}
		// Check SpanSet (for non-structural queries)
		if trace.SpanSet != nil {
			spansCount += len(trace.SpanSet.Spans)
		}
	}
	return spansCount
}

// filterMatchingEntries filters execution plan entries by query name
func filterMatchingEntries(executionPlan []config.PlanEntry, queryName string) []config.PlanEntry {
	var matchingEntries []config.PlanEntry
	for _, entry := range executionPlan {
		if entry.QueryName == queryName {
			matchingEntries = append(matchingEntries, entry)
		}
	}
	return matchingEntries
}

// Run executes the worker's main loop, performing queries according to the execution plan
func (w *Worker) Run(initialDelay time.Duration) {
	// Initial delay to spread workers
	time.Sleep(initialDelay)

	// Filter plan entries for this query name (computed once before loop)
	matchingEntries := filterMatchingEntries(w.executionPlan, w.name)

	for {
		// Wait for rate limiter permission (blocks until allowed)
		if err := w.limiter.Wait(w.ctx); err != nil {
			slog.Error("rate limiter error", "worker_id", w.workerID, "error", err)
			return
		}

		// Determine bucket name and time range using execution plan from config
		bucketName := "immediate"
		var startTime, endTime time.Time
		var bucket *config.TimeBucket

		if len(matchingEntries) > 0 {
			// Get or create index counter for this query
			planIdx := getPlanIndex(w.name)
			idx := atomic.AddInt64(planIdx, 1) - 1
			entryIdx := int(idx) % len(matchingEntries) // Cycle through matching entries - repeats when exhausted
			entry := matchingEntries[entryIdx]

			// Log when we've cycled through all entries once
			if idx > 0 && idx%int64(len(matchingEntries)) == 0 {
				slog.Info("cycled through all plan entries",
					"worker_id", w.workerID,
					"query", w.name,
					"entry_count", len(matchingEntries),
					"cycle", idx/int64(len(matchingEntries)))
			}

			bucketName = entry.BucketName
			if bucketName != "immediate" {
				// Find the bucket by name
				for i := range w.timeBuckets {
					if w.timeBuckets[i].Name == bucketName {
						bucket = &w.timeBuckets[i]
						break
					}
				}

				if bucket != nil {
					// Check if bucket is eligible based on elapsed time
					elapsed := time.Since(w.testStartTime)
					if bucket.AgeEnd <= elapsed {
						// Calculate time range dynamically without jitter for stable query windows
						now := time.Now()
						// Use fixed bucket boundaries for consistent results
						endTime = now.Add(-bucket.AgeStart)
						startTime = now.Add(-bucket.AgeEnd)
					} else {
						// Bucket not eligible yet, use immediate
						bucket = nil
						bucketName = "immediate"
					}
				} else {
					// Bucket not found, use immediate
					bucketName = "immediate"
					slog.Warn("bucket not found in timeBuckets config, using immediate", "worker_id", w.workerID, "bucket", bucketName)
				}
			}
		} else {
			// No matching entries in plan for this query - this shouldn't happen if config is valid
			slog.Warn("no plan entries for query, using immediate bucket", "worker_id", w.workerID, "query", w.name)
		}

		// Prepare time range parameters for client
		var startTimePtr, endTimePtr *time.Time
		if bucket != nil {
			startTimePtr = &startTime
			endTimePtr = &endTime
		}

		// Perform Tempo search using client
		start := time.Now()
		searchResp, res, err := w.tempoClient.Search(w.ctx, w.traceQL, startTimePtr, endTimePtr, w.limit)

		queryDuration := time.Since(start).Seconds()
		queryName := w.name
		w.metrics.QueryLatencyHist.WithLabelValues(queryName).Observe(queryDuration)
		w.metrics.BucketDurationHist.WithLabelValues(bucketName, queryName).Observe(queryDuration)
		w.metrics.BucketQueryCounter.WithLabelValues(bucketName, queryName).Inc()

		if err != nil {
			slog.Error("error making search request", "worker_id", w.workerID, "error", err)
			w.metrics.QueryFailuresCounter.WithLabelValues(queryName, "0").Inc()
			continue
		}

		if res.StatusCode >= 300 {
			w.metrics.QueryFailuresCounter.WithLabelValues(queryName, strconv.Itoa(res.StatusCode)).Inc()
			slog.Error("query failed", "worker_id", w.workerID, "bucket", bucketName, "status_code", res.StatusCode)
			continue
		}

		// Count spans from parsed response
		spansCount := countSpansFromResponse(searchResp)

		// Always record spans returned metric (0 if parsing failed, actual count otherwise)
		w.metrics.SpansReturnedHist.WithLabelValues(queryName).Observe(float64(spansCount))

		// Format log message with or without time range
		if bucket != nil {
			slog.Info("query completed",
				"worker_id", w.workerID,
				"bucket", bucketName,
				"query", w.name,
				"duration_seconds", queryDuration,
				"status_code", res.StatusCode,
				"spans", spansCount,
				"start_time", startTime.Format("15:04:05"),
				"end_time", endTime.Format("15:04:05"))
		} else {
			slog.Info("query completed",
				"worker_id", w.workerID,
				"bucket", bucketName,
				"query", w.name,
				"duration_seconds", queryDuration,
				"status_code", res.StatusCode,
				"spans", spansCount,
				"note", "immediate data, no time range")
		}
		// Rate limiter will control the next iteration
	}
}
