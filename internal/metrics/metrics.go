package metrics

import (
	"log/slog"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the query load generator
type Metrics struct {
	// Query latency histogram with query name label
	QueryLatencyHist *prometheus.HistogramVec

	// Query failures counter with query name label
	QueryFailuresCounter *prometheus.CounterVec

	// Time bucket query counter
	BucketQueryCounter *prometheus.CounterVec

	// Time bucket duration histogram
	BucketDurationHist *prometheus.HistogramVec

	// Spans returned histogram with query name label
	SpansReturnedHist *prometheus.HistogramVec

	// Trace fetch latency histogram
	TraceFetchLatencyHist *prometheus.HistogramVec

	// Trace fetch failures counter
	TraceFetchFailuresCounter *prometheus.CounterVec

	// Response size histogram with type and query name labels
	ResponseSizeBytesHist *prometheus.HistogramVec

	// Active workers gauge
	ActiveWorkersGauge prometheus.Gauge

	// Current target QPS (changes during ramp-up)
	TargetQPSGauge prometheus.Gauge

	// Rolling achieved QPS (last 10s window)
	AchievedQPSGauge prometheus.Gauge

	// Total queries executed
	TotalQueriesCounter prometheus.Counter

	// Current phase (0=ramp-up, 1=steady-state, 2=shutdown)
	TestPhaseGauge prometheus.Gauge

	// Test metadata info metric with labels
	TestMetadataGauge *prometheus.GaugeVec
}

// NewMetrics initializes all Prometheus metrics once at startup
func NewMetrics(namespace string) *Metrics {
	// Sanitize namespace for metric names
	sanitizedNs := strings.ReplaceAll(namespace, "-", "_")

	// Query latency histogram with query name label
	queryLatencyHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "query_load_test",
		Name:      sanitizedNs,
		Help:      "Query latency in seconds",
	}, []string{"name"})

	// Query failures counter with query name label
	queryFailuresCounter := promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "query_failures_count",
		Name:      sanitizedNs,
		Help:      "Total query failures",
	}, []string{"name", "status_code"})

	// Time bucket query counter
	bucketQueryCounter := promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "query_load_test",
		Subsystem: "time_bucket",
		Name:      "queries_total",
		Help:      "Total queries executed per time bucket",
	}, []string{"bucket", "query_name"})

	// Time bucket duration histogram
	bucketDurationHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "query_load_test",
		Subsystem: "time_bucket",
		Name:      "duration_seconds",
		Help:      "Query duration per time bucket",
	}, []string{"bucket", "query_name"})

	// Spans returned histogram with query name label
	spansReturnedHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "query_load_test",
		Subsystem: "spans_returned",
		Name:      sanitizedNs,
		Help:      "Number of spans returned per query",
		Buckets:   []float64{0, 10, 50, 100, 250, 500, 1000, 2500, 5000},
	}, []string{"name"})

	// Trace fetch latency histogram
	traceFetchLatencyHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "query_load_test",
		Subsystem: "trace_fetch",
		Name:      "latency_seconds",
		Help:      "Trace fetch latency in seconds",
	}, []string{"query_name"})

	// Trace fetch failures counter
	traceFetchFailuresCounter := promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "query_load_test",
		Subsystem: "trace_fetch",
		Name:      "failures_total",
		Help:      "Total trace fetch failures",
	}, []string{"query_name", "status_code"})

	// Response size histogram with type and query name labels
	responseSizeBytesHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "query_load_test",
		Subsystem: "response",
		Name:      "size_bytes",
		Help:      "Size of response in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 16), // Start at 1KB, up to ~64MB
	}, []string{"type", "query_name"})

	// Active workers gauge
	activeWorkersGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "query_load_test",
		Subsystem: "workers",
		Name:      "active",
		Help:      "Number of workers currently executing a query",
	})

	// Target QPS gauge
	targetQPSGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "query_load_test",
		Subsystem: "test",
		Name:      "target_qps",
		Help:      "Current target QPS (changes during ramp-up)",
	})

	// Achieved QPS gauge
	achievedQPSGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "query_load_test",
		Subsystem: "test",
		Name:      "achieved_qps",
		Help:      "Rolling achieved QPS (last 10s window)",
	})

	// Total queries counter
	totalQueriesCounter := promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "query_load_test",
		Subsystem: "test",
		Name:      "queries_total",
		Help:      "Total queries executed",
	})

	// Test phase gauge
	testPhaseGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "query_load_test",
		Subsystem: "test",
		Name:      "phase",
		Help:      "Current test phase (0=ramp-up, 1=steady-state, 2=shutdown)",
	})

	// Test metadata gauge
	testMetadataGauge := promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "query_load_test",
		Subsystem: "test",
		Name:      "metadata",
		Help:      "Test metadata (start_time, target_qps, concurrency)",
	}, []string{"start_time", "target_qps", "concurrency"})

	slog.Info("metrics initialized", "namespace", namespace, "sanitized_namespace", sanitizedNs)

	return &Metrics{
		QueryLatencyHist:          queryLatencyHist,
		QueryFailuresCounter:      queryFailuresCounter,
		BucketQueryCounter:        bucketQueryCounter,
		BucketDurationHist:        bucketDurationHist,
		SpansReturnedHist:         spansReturnedHist,
		TraceFetchLatencyHist:     traceFetchLatencyHist,
		TraceFetchFailuresCounter: traceFetchFailuresCounter,
		ResponseSizeBytesHist:     responseSizeBytesHist,
		ActiveWorkersGauge:        activeWorkersGauge,
		TargetQPSGauge:            targetQPSGauge,
		AchievedQPSGauge:          achievedQPSGauge,
		TotalQueriesCounter:       totalQueriesCounter,
		TestPhaseGauge:            testPhaseGauge,
		TestMetadataGauge:         testMetadataGauge,
	}
}
