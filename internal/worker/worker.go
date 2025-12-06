package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rubenvp8510/tempo-query-generator/internal/client"
	"github.com/rubenvp8510/tempo-query-generator/internal/config"
	"github.com/rubenvp8510/tempo-query-generator/internal/metrics"
)

const (
	minAdaptiveBackoff = 200 * time.Millisecond
	maxAdaptiveBackoff = 30 * time.Second
	backoffJitterCap   = 300 * time.Millisecond
)

// RateLimiter interface allows both rate.Limiter and dynamic limiters
type RateLimiter interface {
	Wait(ctx context.Context) error
}

// Worker handles individual query execution in a concurrent worker
type Worker struct {
	workerID              int
	tempoClient           *client.TempoClient
	limiter               RateLimiter
	queries               map[string]config.QueryDefinition
	executionPlan         []config.PlanEntry
	timeBuckets           []config.TimeBucket
	bucketWeightMap       map[string]int // Map bucket names to weights for quick lookup
	metrics               *metrics.Metrics
	limit                 int
	testStartTime         time.Time
	rng                   *rand.Rand    // Deterministic random number generator
	traceFetchProbability float64       // Probability of fetching full trace after search
	jitter                time.Duration // Maximum jitter to apply to time windows
	backoffDuration       time.Duration // Current adaptive backoff delay
}

// bucketResolution holds the resolved bucket information
type bucketResolution struct {
	bucket     *config.TimeBucket
	bucketName string
	startTime  *time.Time
	endTime    *time.Time
}

// Run executes the worker's main loop, selecting from execution plan using weighted random selection
func (w *Worker) Run(ctx context.Context, initialDelay time.Duration) {
	// Initial delay to spread workers (use deterministic delay based on RNG)
	if initialDelay > 0 {
		delay := time.Duration(w.rng.Int63n(int64(initialDelay)))
		select {
		case <-ctx.Done():
			slog.Info("worker cancelled during initial delay", "worker_id", w.workerID)
			return
		case <-time.After(delay):
			// Continue
		}
	}

	for {
		// Check if context is cancelled before starting new query
		select {
		case <-ctx.Done():
			slog.Info("worker shutting down", "worker_id", w.workerID)
			return
		default:
			// Continue with query execution
		}

		// Wait for rate limiter permission (blocks until allowed)
		if err := w.limiter.Wait(ctx); err != nil {
			if err == context.Canceled {
				slog.Info("worker stopped during rate limit wait", "worker_id", w.workerID)
				return
			}
			slog.Error("rate limiter error", "worker_id", w.workerID, "error", err)
			return
		}

		// Mark worker as active
		w.metrics.ActiveWorkersGauge.Inc()

		// Select plan entry using weighted random selection
		entry := w.selectWeightedPlanEntry()
		if entry == nil {
			w.metrics.ActiveWorkersGauge.Dec()
			continue
		}

		// Resolve query definition and bucket
		queryDef, bucketRes := w.resolveQueryAndBucket(entry)
		if queryDef == nil {
			w.metrics.ActiveWorkersGauge.Dec()
			continue
		}

		// Execute search query
		searchResp, res, queryDuration, spansCount := w.executeSearch(ctx, queryDef, bucketRes, entry.QueryName)
		if res != nil && (res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500) {
			w.applyAdaptiveBackoff(ctx, res, entry.QueryName, bucketRes.bucketName)
		} else if res != nil && res.StatusCode < 300 {
			// Successful response clears any accumulated backoff
			w.resetBackoff()
		}

		if searchResp == nil {
			w.metrics.ActiveWorkersGauge.Dec()
			continue
		}

		// Fetch full trace details probabilistically
		w.getTrace(ctx, searchResp, entry.QueryName)

		// Increment total queries counter
		w.metrics.TotalQueriesCounter.Inc()

		// Log query completion
		w.logQueryCompletion(entry.QueryName, bucketRes, queryDuration, res.StatusCode, spansCount)

		// Mark worker as idle
		w.metrics.ActiveWorkersGauge.Dec()
	}
}

// selectWeightedPlanEntry selects a plan entry using weighted random selection based on bucket weights
func (w *Worker) selectWeightedPlanEntry() *config.PlanEntry {
	if len(w.executionPlan) == 0 {
		return nil
	}

	// Filter eligible plan entries (those whose buckets have elapsed enough time)
	eligibleEntries := make([]config.PlanEntry, 0)
	elapsed := time.Since(w.testStartTime)

	for _, entry := range w.executionPlan {
		// Find the bucket for this entry
		var bucket *config.TimeBucket
		for i := range w.timeBuckets {
			if w.timeBuckets[i].Name == entry.BucketName {
				bucket = &w.timeBuckets[i]
				break
			}
		}

		// If bucket not found or not eligible yet, skip this entry
		if bucket == nil {
			continue
		}

		// Check if bucket is eligible (has elapsed enough time)
		if bucket.AgeEnd <= elapsed {
			eligibleEntries = append(eligibleEntries, entry)
		}
	}

	// If no entries are eligible, return first entry (will use immediate bucket in resolveQueryAndBucket)
	if len(eligibleEntries) == 0 {
		if len(w.executionPlan) > 0 {
			// Return first entry but it will use immediate bucket
			return &w.executionPlan[0]
		}
		return nil
	}

	// Calculate total weight of eligible entries
	totalWeight := 0
	for _, entry := range eligibleEntries {
		weight := w.bucketWeightMap[entry.BucketName]
		if weight <= 0 {
			weight = 1 // Default weight if not found
		}
		totalWeight += weight
	}

	if totalWeight == 0 {
		// All weights are 0, select uniformly
		selectedIdx := w.rng.Intn(len(eligibleEntries))
		return &eligibleEntries[selectedIdx]
	}

	// Weighted random selection
	randValue := w.rng.Intn(totalWeight)
	currentWeight := 0
	for _, entry := range eligibleEntries {
		weight := w.bucketWeightMap[entry.BucketName]
		if weight <= 0 {
			weight = 1 // Default weight if not found
		}
		currentWeight += weight
		if randValue < currentWeight {
			return &entry
		}
	}

	// Fallback to last eligible entry (shouldn't happen)
	return &eligibleEntries[len(eligibleEntries)-1]
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

	// Calculate time range with jitter to defeat caching
	now := time.Now()

	// Apply random jitter if configured (shifts the entire window to randomize cache keys)
	var shift time.Duration
	if w.jitter > 0 {
		// Generate random float between -1.0 and 1.0
		randomFactor := (w.rng.Float64() * 2) - 1
		// Apply the same shift to both start and end to preserve the window duration
		// but randomize the absolute position
		shift = time.Duration(randomFactor * float64(w.jitter))
	}

	endTime := now.Add(-bucket.AgeStart).Add(shift)
	startTime := now.Add(-bucket.AgeEnd).Add(shift)

	res.bucket = bucket
	res.bucketName = bucketName
	res.startTime = &startTime
	res.endTime = &endTime

	return res
}

// executeSearch performs the Tempo search and records metrics
func (w *Worker) executeSearch(ctx context.Context, queryDef *config.QueryDefinition, bucketRes *bucketResolution, queryName string) (*client.TempoSearchResponse, *http.Response, float64, int) {
	// Perform Tempo search using client
	start := time.Now()
	searchResp, res, bodySize, err := w.tempoClient.Search(ctx, queryDef.TraceQL, bucketRes.startTime, bucketRes.endTime, w.limit)

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

	// Record response size
	w.metrics.ResponseSizeBytesHist.WithLabelValues("search", queryName).Observe(float64(bodySize))

	return searchResp, res, queryDuration, spansCount
}

// getTrace probabilistically fetches the full trace details after a search
func (w *Worker) getTrace(ctx context.Context, searchResp *client.TempoSearchResponse, queryName string) {
	if w.traceFetchProbability <= 0 || searchResp == nil || len(searchResp.Traces) == 0 {
		return
	}

	// Generate random number to determine if we should fetch the trace (using worker's deterministic RNG)
	if w.rng.Float64() >= w.traceFetchProbability {
		return
	}

	// Extract traceID from the first result
	traceID := searchResp.Traces[0].TraceID
	if traceID == "" {
		return
	}

	// Fetch the full trace
	fetchStart := time.Now()
	fetchRes, bodySize, err := w.tempoClient.GetTrace(ctx, traceID)
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
		if fetchRes.StatusCode == http.StatusTooManyRequests || fetchRes.StatusCode >= 500 {
			w.applyAdaptiveBackoff(ctx, fetchRes, queryName, "trace_fetch")
		}
		return
	}

	// Record response size
	w.metrics.ResponseSizeBytesHist.WithLabelValues("trace", queryName).Observe(float64(bodySize))

	slog.Debug("trace fetched", "worker_id", w.workerID, "query", queryName, "trace_id", traceID, "duration_seconds", fetchDuration)
}

// applyAdaptiveBackoff backs off the worker when Tempo signals throttling or overload
func (w *Worker) applyAdaptiveBackoff(ctx context.Context, res *http.Response, queryName, bucketName string) {
	backoff := w.nextBackoffDuration(res)
	if backoff <= 0 {
		return
	}

	slog.Warn("adaptive backoff triggered",
		"worker_id", w.workerID,
		"query", queryName,
		"bucket", bucketName,
		"status_code", res.StatusCode,
		"backoff", backoff.String())

	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		slog.Info("backoff cancelled", "worker_id", w.workerID, "query", queryName)
		return
	case <-timer.C:
	}
}

// nextBackoffDuration calculates the next delay using Retry-After when available or capped exponential backoff
func (w *Worker) nextBackoffDuration(res *http.Response) time.Duration {
	if res == nil {
		return 0
	}

	if retryAfter := parseRetryAfter(res); retryAfter > 0 {
		w.backoffDuration = retryAfter
		return retryAfter + w.jitterDuration(backoffJitterCap)
	}

	if w.backoffDuration == 0 {
		w.backoffDuration = minAdaptiveBackoff
	} else {
		w.backoffDuration *= 2
		if w.backoffDuration > maxAdaptiveBackoff {
			w.backoffDuration = maxAdaptiveBackoff
		}
	}

	return w.backoffDuration + w.jitterDuration(backoffJitterCap)
}

// resetBackoff clears any accumulated adaptive backoff
func (w *Worker) resetBackoff() {
	w.backoffDuration = 0
}

// parseRetryAfter returns the delay specified by the Retry-After header
func parseRetryAfter(res *http.Response) time.Duration {
	value := strings.TrimSpace(res.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}

	if t, err := http.ParseTime(value); err == nil {
		delay := time.Until(t)
		if delay < 0 {
			return 0
		}
		return delay
	}

	return 0
}

// jitterDuration returns a random duration up to max to avoid synchronized retries
func (w *Worker) jitterDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(w.rng.Int63n(int64(max)))
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
