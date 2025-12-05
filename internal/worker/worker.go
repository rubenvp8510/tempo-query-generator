package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
	"github.com/rubenvp8510/tempo-query-generator/internal/metrics"
)

// Worker handles individual query execution in a concurrent worker
type Worker struct {
	workerID              int
	tempoClient           *client.TempoClient
	limiter               *rate.Limiter
	queries               map[string]config.QueryDefinition
	timeBuckets           []config.TimeBucket
	executionPlan         []config.PlanEntry
	metrics               *metrics.Metrics
	limit                 int
	testStartTime         time.Time
	ctx                   context.Context
	planIndex             int64   // Per-worker counter for cycling through execution plan
	traceFetchProbability float64 // Probability of fetching full trace after search
}

// bucketResolution holds the resolved bucket information
type bucketResolution struct {
	bucket     *config.TimeBucket
	bucketName string
	startTime  *time.Time
	endTime    *time.Time
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

		// Get next plan entry
		entry := w.getNextPlanEntry()
		if entry == nil {
			continue
		}

		// Resolve query definition and bucket
		queryDef, bucketRes := w.resolveQueryAndBucket(entry)
		if queryDef == nil {
			continue
		}

		// Execute search query
		searchResp, res, queryDuration, spansCount := w.executeSearch(queryDef, bucketRes, entry.QueryName)
		if searchResp == nil {
			continue
		}

		// Fetch full trace details probabilistically
		w.getTrace(searchResp, entry.QueryName)

		// Log query completion
		w.logQueryCompletion(entry.QueryName, bucketRes, queryDuration, res.StatusCode, spansCount)
	}
}

// getNextPlanEntry gets the next plan entry using per-worker counter for deterministic cycling
func (w *Worker) getNextPlanEntry() *config.PlanEntry {
	idx := atomic.AddInt64(&w.planIndex, 1) - 1
	entryIdx := int(idx) % len(w.executionPlan)
	return &w.executionPlan[entryIdx]
}

// resolveQueryAndBucket resolves the query definition and bucket from the plan entry
func (w *Worker) resolveQueryAndBucket(entry *config.PlanEntry) (*config.QueryDefinition, *bucketResolution) {
	// Look up query definition from map
	queryDef, exists := w.queries[entry.QueryName]
	if !exists {
		slog.Error("query not found in queries map", "worker_id", w.workerID, "query_name", entry.QueryName)
		w.metrics.QueryFailuresCounter.WithLabelValues(entry.QueryName, "0").Inc()
		return nil, nil
	}

	// Resolve bucket
	bucketRes := w.resolveBucket(entry.BucketName)

	return &queryDef, bucketRes
}

// resolveBucket resolves the bucket name to a bucket with time range
func (w *Worker) resolveBucket(bucketName string) *bucketResolution {
	res := &bucketResolution{
		bucketName: "immediate",
	}

	if bucketName == "immediate" {
		return res
	}

	// Find the bucket by name
	var bucket *config.TimeBucket
	for i := range w.timeBuckets {
		if w.timeBuckets[i].Name == bucketName {
			bucket = &w.timeBuckets[i]
			break
		}
	}

	if bucket == nil {
		slog.Warn("bucket not found in timeBuckets config, using immediate", "worker_id", w.workerID, "bucket", bucketName)
		return res
	}

	// Check if bucket is eligible based on elapsed time
	elapsed := time.Since(w.testStartTime)
	if bucket.AgeEnd > elapsed {
		// Bucket not eligible yet, use immediate
		return res
	}

	// Calculate time range dynamically without jitter for stable query windows
	now := time.Now()
	// Use fixed bucket boundaries for consistent results
	endTime := now.Add(-bucket.AgeStart)
	startTime := now.Add(-bucket.AgeEnd)

	res.bucket = bucket
	res.bucketName = bucketName
	res.startTime = &startTime
	res.endTime = &endTime

	return res
}

// executeSearch performs the Tempo search and records metrics
func (w *Worker) executeSearch(queryDef *config.QueryDefinition, bucketRes *bucketResolution, queryName string) (*client.TempoSearchResponse, *http.Response, float64, int) {
	// Perform Tempo search using client
	start := time.Now()
	searchResp, res, err := w.tempoClient.Search(w.ctx, queryDef.TraceQL, bucketRes.startTime, bucketRes.endTime, w.limit)

	queryDuration := time.Since(start).Seconds()

	// Record metrics
	w.metrics.QueryLatencyHist.WithLabelValues(queryName).Observe(queryDuration)
	w.metrics.BucketDurationHist.WithLabelValues(bucketRes.bucketName, queryName).Observe(queryDuration)
	w.metrics.BucketQueryCounter.WithLabelValues(bucketRes.bucketName, queryName).Inc()

	if err != nil {
		slog.Error("error making search request", "worker_id", w.workerID, "query", queryName, "error", err)
		w.metrics.QueryFailuresCounter.WithLabelValues(queryName, "0").Inc()
		return nil, nil, queryDuration, 0
	}

	if res.StatusCode >= 300 {
		w.metrics.QueryFailuresCounter.WithLabelValues(queryName, strconv.Itoa(res.StatusCode)).Inc()
		slog.Error("query failed", "worker_id", w.workerID, "query", queryName, "bucket", bucketRes.bucketName, "status_code", res.StatusCode)
		return nil, res, queryDuration, 0
	}

	// Count spans from parsed response
	spansCount := countSpansFromResponse(searchResp)

	// Always record spans returned metric (0 if parsing failed, actual count otherwise)
	w.metrics.SpansReturnedHist.WithLabelValues(queryName).Observe(float64(spansCount))

	return searchResp, res, queryDuration, spansCount
}

// getTrace probabilistically fetches the full trace details after a search
func (w *Worker) getTrace(searchResp *client.TempoSearchResponse, queryName string) {
	if w.traceFetchProbability <= 0 || searchResp == nil || len(searchResp.Traces) == 0 {
		return
	}

	// Generate random number to determine if we should fetch the trace
	if rand.Float64() >= w.traceFetchProbability {
		return
	}

	// Extract traceID from the first result
	traceID := searchResp.Traces[0].TraceID
	if traceID == "" {
		return
	}

	// Fetch the full trace
	fetchStart := time.Now()
	fetchRes, err := w.tempoClient.GetTrace(w.ctx, traceID)
	fetchDuration := time.Since(fetchStart).Seconds()
	w.metrics.TraceFetchLatencyHist.WithLabelValues(queryName).Observe(fetchDuration)

	if err != nil {
		slog.Error("error fetching trace", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "error", err)
		w.metrics.TraceFetchFailuresCounter.WithLabelValues(queryName, "0").Inc()
		return
	}

	if fetchRes.StatusCode >= 300 {
		w.metrics.TraceFetchFailuresCounter.WithLabelValues(queryName, strconv.Itoa(fetchRes.StatusCode)).Inc()
		slog.Error("trace fetch failed", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "status_code", fetchRes.StatusCode)
		return
	}

	slog.Debug("trace fetched", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "duration_seconds", fetchDuration)
}

// logQueryCompletion logs the query completion with appropriate details
func (w *Worker) logQueryCompletion(queryName string, bucketRes *bucketResolution, queryDuration float64, statusCode int, spansCount int) {
	if bucketRes.bucket != nil {
		slog.Info("query completed",
			"worker_id", w.workerID,
			"bucket", bucketRes.bucketName,
			"query", queryName,
			"duration_seconds", queryDuration,
			"status_code", statusCode,
			"spans", spansCount,
			"start_time", bucketRes.startTime.Format("15:04:05"),
			"end_time", bucketRes.endTime.Format("15:04:05"))
	} else {
		slog.Info("query completed",
			"worker_id", w.workerID,
			"bucket", bucketRes.bucketName,
			"query", queryName,
			"duration_seconds", queryDuration,
			"status_code", statusCode,
			"spans", spansCount,
			"note", "immediate data, no time range")
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
