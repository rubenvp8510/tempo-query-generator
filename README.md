# Tempo Query Load Generator

A Go-based tool for generating controlled query load against Tempo's search API. This tool is designed to simulate realistic query patterns for performance testing and capacity planning. It supports both search queries and full trace retrieval, simulating real-world user workflows where users search for traces and then view detailed trace information.

## Table of Contents

- [Methodology for Generated Query Load](#methodology-for-generated-query-load)
- [How to Use the Tool](#how-to-use-the-tool)
- [Code Organization](#code-organization)

## Methodology for Generated Query Load

The query load generator uses a sophisticated approach to create realistic and controlled query patterns:

### Rate Limiting and QPS Control

- **Target QPS**: The tool achieves a total target queries per second (QPS) by distributing the load across all concurrent workers. Each worker receives an equal share: `perWorkerQPS = targetQPS / totalConcurrency`.
- **Rate Limiter**: Uses `golang.org/x/time/rate` to enforce precise rate limiting. Each worker has its own independent rate limiter to ensure fair distribution and predictable behavior.
- **Burst Multiplier**: Allows temporary bursts above the target rate to compensate for network latency and query execution time. The burst size is calculated as `perWorkerQPS * burstMultiplier`.
- **QPS Multiplier**: Optional multiplier applied to the target QPS for compensation scenarios (default: 1.0).

### Time Bucket Distribution

The tool uses a time bucket system to distribute queries across different time ranges, simulating realistic query patterns:

- **Time Buckets**: Defined in configuration with `ageStart` and `ageEnd` parameters representing how far back in time to query.
- **Bucket Eligibility**: Buckets are only used when enough time has elapsed since test start (when `ageEnd <= elapsed time`).
- **Execution Plan**: Each query type has an associated execution plan that specifies which time bucket to use for each query execution.
- **Cycling**: The execution plan cycles through entries in order, repeating when exhausted, ensuring consistent distribution patterns.

### Concurrency Model

- **Total Concurrency Control**: Configure the exact number of concurrent workers via `totalConcurrency` (e.g., `totalConcurrency: 50` creates exactly 50 concurrent workers).
- **Per-Worker Rate Limiting**: Each worker has an independent rate limiter set to `targetQPS / totalConcurrency` for fair distribution and predictable behavior.
- **Staggered Start Positions**: Workers start at evenly distributed positions in the execution plan to ensure immediate query diversity and reduce cache effects.
- **Initial Delay**: Workers start with a small random delay (0-1 second) to spread the initial load and avoid thundering herd effects.

### Query Distribution

- **Deterministic Cycling**: Workers cycle through all execution plan entries in order, with each worker maintaining its own position in the cycle.
- **Staggered Positions**: Workers start at evenly distributed positions: worker N starts at `(N * planLength) / totalWorkers`, ensuring immediate query diversity.
- **Plan-Based Distribution**: Query frequency is determined by the execution plan - queries with more entries in the plan are executed more frequently.
- **Reproducible Patterns**: The same configuration produces the same query pattern across test runs, enabling reliable performance comparisons.

### Search-and-Fetch Workflow

The tool simulates realistic user behavior by implementing a search-and-fetch pattern:

- **Search Phase**: Workers execute TraceQL search queries to find traces matching specific criteria.
- **Fetch Phase**: After a successful search, workers probabilistically fetch the full trace details by trace ID, simulating a user clicking on a search result.
- **Click Probability**: Configurable probability (0.0-1.0) determines how often full traces are fetched after searches. A value of 0.5 means 50% of successful searches will trigger a trace fetch.
- **Realistic Load**: This two-phase approach exercises both Tempo's search index and trace retrieval mechanisms, providing a more representative performance test.

### Metrics Collection

The tool exposes Prometheus metrics on port 2112 (`/metrics` endpoint) for monitoring:
- Query latency histograms
- Query failure counters
- Time bucket query counters
- Spans returned histograms
- Trace fetch latency histograms
- Trace fetch failure counters

## How to Use the Tool

### Configuration

The tool is configured via a YAML file (default: `/config/config.yaml`). The configuration file path can be overridden using the `CONFIG_FILE` environment variable or `TEMPO_CONFIG_FILE`.

#### Configuration Structure

```yaml
tempo:
  queryEndpoint: "https://tempo-simplest-gateway:8080"

namespace: "tempo-perf-test"
tenantId: "tenant-1"

query:
  delay: "5s"                    # Delay between query cycles (not actively used in current implementation)
  totalConcurrency: 50            # Total number of concurrent workers across all queries
  targetQPS: 50                   # Total queries per second across all workers
  burstMultiplier: 2.0            # Rate limiter burst multiplier (per-worker burst = perWorkerQPS * burstMultiplier)
  qpsMultiplier: 1.0             # Optional QPS compensation multiplier
  limit: 1000                     # Maximum results per query
  clickProbability: 0.5          # Probability of fetching full trace after search (0.0-1.0, default: 0.5)

timeBuckets:
  - name: "recent"
    ageStart: "10s"               # Query window ends this far back
    ageEnd: "1m"                  # Query window starts this far back
    weight: 50                    # Weight for random selection (if used)
  - name: "ingester"
    ageStart: "1m"
    ageEnd: "5m"
    weight: 40
  - name: "backend"
    ageStart: "5m"
    ageEnd: "15m"
    weight: 10

queries:
  - name: "resource_service_loadtest"
    traceql: '{ resource.service.name = "frontend" }'
  # ... more queries

executionPlan:
  - queryName: "resource_service_loadtest"
    bucketName: "recent"
  # ... more plan entries
```

#### Environment Variables

The configuration file path can be set using environment variables:

- `TEMPO_CONFIG_FILE` or `CONFIG_FILE` - Path to configuration file (default: `/config/config.yaml`)

### Building

#### Using Make

```bash
# Build the binary
make build

# Build Docker image
make image-build

# Build and push Docker image
make image-push
```

#### Manual Build

```bash
go build -o query-load-generator ./cmd/query-load-generator
```

### Running

#### Local Execution

```bash
# Using Make (sets CONFIG_FILE automatically)
make run

# Or directly with Go
CONFIG_FILE=config.yaml go run ./cmd/query-load-generator

# Or with the built binary
CONFIG_FILE=config.yaml ./query-load-generator
```

#### Docker Execution

```bash
# Build the image
docker build -f Dockerfile -t tempo-query-generator .

# Run with custom config
docker run -v $(pwd)/config.yaml:/config/config.yaml tempo-query-generator

# Or with environment variable override
docker run -e TEMPO_CONFIG_FILE=/custom/path/config.yaml \
           -v $(pwd)/config.yaml:/custom/path/config.yaml \
           tempo-query-generator
```

#### Kubernetes Deployment

The tool includes a Kubernetes deployment manifest in `manifests/deployment.yaml`. Deploy using:

```bash
kubectl apply -f manifests/deployment.yaml
```

### Authentication

The tool supports Kubernetes service account token authentication:
- Automatically attempts to load token from `/var/run/secrets/kubernetes.io/serviceaccount/token`
- Falls back to unauthenticated requests if token is not available
- Token is used as a Bearer token in the `Authorization` header

### Monitoring

The tool exposes Prometheus metrics on port 2112:

```bash
# View metrics
curl http://localhost:2112/metrics
```

Key metrics:
- `query_load_test_<namespace>_<query_name>` - Query latency histogram
- `query_failures_count_<namespace>_<query_name>_<status_code>` - Query failure counter
- `query_load_test_time_bucket_queries_total` - Queries per time bucket
- `query_load_test_time_bucket_duration_seconds` - Duration per time bucket
- `query_load_test_spans_returned_<namespace>_<query_name>` - Spans returned histogram
- `query_load_test_trace_fetch_latency_seconds{query_name}` - Trace fetch latency histogram
- `query_load_test_trace_fetch_failures_total{query_name,status_code}` - Trace fetch failure counter

## Code Organization

The codebase is organized into several packages following Go best practices:

### Package Structure

```
tempo-query-generator/
├── cmd/
│   └── query-load-generator/
│       └── main.go              # Application entry point
├── internal/
│   ├── client/
│   │   ├── tempo.go             # Tempo API client
│   │   └── response.go          # Response data structures
│   ├── config/
│   │   ├── config.go            # Configuration loading and validation
│   │   └── types.go             # Configuration type definitions
│   └── generator/
│       ├── executor.go          # Query executor with rate limiting
│       ├── worker.go            # Individual query worker
│       ├── bucket.go            # Time bucket selection logic
│       └── metrics.go           # Prometheus metrics definitions
├── config.yaml                  # Default configuration file
├── Dockerfile                   # Container build definition
├── Makefile                     # Build automation
└── manifests/
    └── deployment.yaml          # Kubernetes deployment manifest
```

### Main Components

#### `cmd/query-load-generator/main.go`

The application entry point that:
- Loads and validates configuration
- Initializes Prometheus metrics
- Creates a query lookup map for all query types
- Creates and starts a single executor managing all workers
- Starts the metrics HTTP server

#### `internal/config/`

**`types.go`**: Defines all configuration data structures:
- `Config` - Main configuration structure
- `TempoConfig` - Tempo-specific settings
- `QueryConfig` - Query execution parameters
- `TimeBucketConfig` - Time bucket definitions
- `QueryDefinition` - Query definitions
- `PlanEntry` - Execution plan entries

**`config.go`**: Handles configuration loading:
- Uses Viper for YAML parsing and environment variable support
- Validates configuration using `go-playground/validator`
- Sets default values for optional fields
- Validates execution plan consistency (queries and buckets must exist)

#### `internal/client/`

**`tempo.go`**: Tempo API client implementation:
- `TempoClient` - HTTP client for Tempo search API
- `Search()` - Performs TraceQL queries with optional time ranges
- `GetTrace()` - Retrieves full trace details by trace ID
- Handles authentication via Bearer tokens
- Supports multi-tenancy via `X-Scope-OrgID` header

**`response.go`**: Defines Tempo API response structures for parsing JSON responses.

#### `internal/generator/`

**`executor.go`**: Manages query execution across all query types:
- `Executor` - Coordinates all workers with total concurrency control
- Calculates per-worker QPS: `targetQPS / totalConcurrency`
- Creates independent rate limiter for each worker for fair distribution
- Spawns workers with staggered starting positions in the execution plan
- Tracks test start time for time bucket eligibility

**`worker.go`**: Individual query execution worker:
- `Worker` - Executes queries in a loop
- Uses builder pattern for flexible construction
- Cycles through all execution plan entries using per-worker counter
- Starts at staggered position for immediate query diversity
- Looks up query definitions dynamically from execution plan
- Selects time buckets based on execution plan
- Records metrics for each query execution
- Handles rate limiting via independent per-worker limiter
- Implements search-and-fetch workflow: after successful searches, probabilistically fetches full trace details

**`bucket.go`**: Time bucket selection logic:
- `SelectTimeBucket()` - Weighted random selection from eligible buckets
- Filters buckets based on elapsed time since test start

**`metrics.go`**: Prometheus metrics definitions:
- `Metrics` - Container for all metric collectors
- Defines histograms and counters for query performance tracking
- Uses configurable namespace for metric namespacing

### Execution Flow

1. **Initialization** (`main.go`):
   - Load configuration from YAML file
   - Validate configuration
   - Initialize Prometheus metrics
   - Calculate per-worker QPS: `targetQPS / totalConcurrency`
   - Create query lookup map from all query definitions

2. **Executor Creation** (`executor.go`):
   - Create single `Executor` managing all queries
   - Calculate per-worker QPS distribution
   - Create Tempo client with authentication

3. **Worker Spawning** (`executor.go`):
   - Spawn `totalConcurrency` workers total
   - Each worker gets independent rate limiter at per-worker QPS
   - Calculate staggered starting position: `(workerID * planLength) / totalWorkers`
   - Each worker starts with random initial delay (0-1 second)
   - Workers run concurrently in separate goroutines

4. **Query Execution Loop** (`worker.go`):
   - Worker waits for its independent rate limiter permission
   - Increments per-worker counter to get next plan entry index
   - Cycles through all execution plan entries
   - Looks up query definition by name from plan entry
   - Determines time bucket and calculates time range
   - Executes search query via Tempo client
   - Records metrics (latency, spans, failures)
   - **Search-and-Fetch**: If search succeeds and `clickProbability` threshold is met:
     - Extracts trace ID from first search result
     - Fetches full trace details via `GetTrace()` API
     - Records trace fetch metrics (latency, failures)
   - Repeats loop

5. **Metrics Collection**:
   - All metrics are automatically exposed via Prometheus
   - Available at `http://localhost:2112/metrics`
