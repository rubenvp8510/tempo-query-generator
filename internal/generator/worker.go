package generator

import (
	"context"
	"log/slog"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
)

// Worker handles individual query execution in a concurrent worker
type Worker struct {
	workerID        int
	tempoClient     *client.TempoClient
	limiter         *rate.Limiter
	queries         map[string]config.QueryDefinition
	timeBuckets     []config.TimeBucket
	executionPlan   []config.PlanEntry
	metrics         *Metrics
	limit           int
	testStartTime   time.Time
	ctx             context.Context
	planIndex       int64   // Per-worker counter for cycling through execution plan
	clickProbability float64 // Probability of fetching full trace after search
}

// WorkerBuilder builds Worker instances using the builder pattern
type WorkerBuilder struct {
	workerID         int
	tempoClient      *client.TempoClient
	limiter          *rate.Limiter
	queries          map[string]config.QueryDefinition
	timeBuckets      []config.TimeBucket
	executionPlan    []config.PlanEntry
	metrics          *Metrics
	limit            int
	testStartTime    time.Time
	initialPlanIndex int64
	clickProbability float64
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

// WithInitialPlanIndex sets the initial plan index for staggered start positions
func (b *WorkerBuilder) WithInitialPlanIndex(initialPlanIndex int64) *WorkerBuilder {
	b.initialPlanIndex = initialPlanIndex
	return b
}

// WithClickProbability sets the probability of fetching full trace after search
func (b *WorkerBuilder) WithClickProbability(clickProbability float64) *WorkerBuilder {
	b.clickProbability = clickProbability
	return b
}

// Build creates a Worker instance from the builder
func (b *WorkerBuilder) Build() *Worker {
	return &Worker{
		workerID:        b.workerID,
		tempoClient:     b.tempoClient,
		limiter:         b.limiter,
		queries:         b.queries,
		timeBuckets:     b.timeBuckets,
		executionPlan:   b.executionPlan,
		metrics:         b.metrics,
		limit:           b.limit,
		testStartTime:   b.testStartTime,
		ctx:             context.Background(),
		planIndex:       b.initialPlanIndex, // Initialize with staggered offset
		clickProbability: b.clickProbability,
	}
}

// NewWorker creates a new worker instance (deprecated: use WorkerBuilder instead)
func NewWorker(
	workerID int,
	tempoClient *client.TempoClient,
	limiter *rate.Limiter,
	queries map[string]config.QueryDefinition,
	timeBuckets []config.TimeBucket,
	executionPlan []config.PlanEntry,
	metrics *Metrics,
	limit int,
	testStartTime time.Time,
	initialPlanIndex int64,
	clickProbability float64,
) *Worker {
	return &Worker{
		workerID:        workerID,
		tempoClient:     tempoClient,
		limiter:         limiter,
		queries:         queries,
		timeBuckets:     timeBuckets,
		executionPlan:   executionPlan,
		metrics:         metrics,
		limit:           limit,
		testStartTime:   testStartTime,
		ctx:             context.Background(),
		planIndex:       initialPlanIndex, // Initialize with staggered offset
		clickProbability: clickProbability,
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

// Run executes the worker's main loop, cycling through all execution plan entries
func (w *Worker) Run(initialDelay time.Duration) {
	// Initial delay to spread workers
	time.Sleep(initialDelay)

	for {
		// Wait for rate limiter permission (blocks until allowed)
		if err := w.limiter.Wait(w.ctx); err != nil {
			slog.Error("rate limiter error", "worker_id", w.workerID, "error", err)
			return
		}

		// Get next plan entry using per-worker counter for deterministic cycling
		idx := atomic.AddInt64(&w.planIndex, 1) - 1
		entryIdx := int(idx) % len(w.executionPlan)
		entry := w.executionPlan[entryIdx]

		// Look up query definition from map
		queryDef, exists := w.queries[entry.QueryName]
		if !exists {
			slog.Error("query not found in queries map", "worker_id", w.workerID, "query_name", entry.QueryName)
			w.metrics.QueryFailuresCounter.WithLabelValues(entry.QueryName, "0").Inc()
			continue
		}

		// Determine bucket name and time range using execution plan
		bucketName := "immediate"
		var startTime, endTime time.Time
		var bucket *config.TimeBucket

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
				slog.Warn("bucket not found in timeBuckets config, using immediate", "worker_id", w.workerID, "bucket", entry.BucketName)
			}
		}

		// Prepare time range parameters for client
		var startTimePtr, endTimePtr *time.Time
		if bucket != nil {
			startTimePtr = &startTime
			endTimePtr = &endTime
		}

		// Perform Tempo search using client
		start := time.Now()
		searchResp, res, err := w.tempoClient.Search(w.ctx, queryDef.TraceQL, startTimePtr, endTimePtr, w.limit)

		queryDuration := time.Since(start).Seconds()
		queryName := entry.QueryName
		w.metrics.QueryLatencyHist.WithLabelValues(queryName).Observe(queryDuration)
		w.metrics.BucketDurationHist.WithLabelValues(bucketName, queryName).Observe(queryDuration)
		w.metrics.BucketQueryCounter.WithLabelValues(bucketName, queryName).Inc()

		if err != nil {
			slog.Error("error making search request", "worker_id", w.workerID, "query", queryName, "error", err)
			w.metrics.QueryFailuresCounter.WithLabelValues(queryName, "0").Inc()
			continue
		}

		if res.StatusCode >= 300 {
			w.metrics.QueryFailuresCounter.WithLabelValues(queryName, strconv.Itoa(res.StatusCode)).Inc()
			slog.Error("query failed", "worker_id", w.workerID, "query", queryName, "bucket", bucketName, "status_code", res.StatusCode)
			continue
		}

		// Count spans from parsed response
		spansCount := countSpansFromResponse(searchResp)

		// Always record spans returned metric (0 if parsing failed, actual count otherwise)
		w.metrics.SpansReturnedHist.WithLabelValues(queryName).Observe(float64(spansCount))

		// Search-and-fetch workflow: simulate user clicking on a trace result
		if w.clickProbability > 0 && searchResp != nil && len(searchResp.Traces) > 0 {
			// Generate random number to determine if we should fetch the trace
			if rand.Float64() < w.clickProbability {
				// Extract traceID from the first result
				traceID := searchResp.Traces[0].TraceID

				if traceID != "" {
					// Fetch the full trace
					fetchStart := time.Now()
					fetchRes, err := w.tempoClient.GetTrace(w.ctx, traceID)
					fetchDuration := time.Since(fetchStart).Seconds()
					w.metrics.TraceFetchLatencyHist.WithLabelValues(queryName).Observe(fetchDuration)

					if err != nil {
						slog.Error("error fetching trace", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "error", err)
						w.metrics.TraceFetchFailuresCounter.WithLabelValues(queryName, "0").Inc()
					} else if fetchRes.StatusCode >= 300 {
						w.metrics.TraceFetchFailuresCounter.WithLabelValues(queryName, strconv.Itoa(fetchRes.StatusCode)).Inc()
						slog.Error("trace fetch failed", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "status_code", fetchRes.StatusCode)
					} else {
						slog.Debug("trace fetched", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "duration_seconds", fetchDuration)
					}
				}
			}
		}

		// Format log message with or without time range
		if bucket != nil {
			slog.Info("query completed",
				"worker_id", w.workerID,
				"bucket", bucketName,
				"query", queryName,
				"duration_seconds", queryDuration,
				"status_code", res.StatusCode,
				"spans", spansCount,
				"start_time", startTime.Format("15:04:05"),
				"end_time", endTime.Format("15:04:05"))
		} else {
			slog.Info("query completed",
				"worker_id", w.workerID,
				"bucket", bucketName,
				"query", queryName,
				"duration_seconds", queryDuration,
				"status_code", res.StatusCode,
				"spans", spansCount,
				"note", "immediate data, no time range")
		}
		// Rate limiter will control the next iteration
	}
}
