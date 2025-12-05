# Tempo Query Load Generator

A Go-based tool for generating controlled query load against Tempo's search API. This tool is designed to simulate realistic query patterns for performance testing and capacity planning.

## Table of Contents

- [Methodology for Generated Query Load](#methodology-for-generated-query-load)
- [How to Use the Tool](#how-to-use-the-tool)
- [Code Organization](#code-organization)

## Methodology for Generated Query Load

The query load generator uses a sophisticated approach to create realistic and controlled query patterns:

### Rate Limiting and QPS Control

- **Target QPS**: The tool distributes a total target queries per second (QPS) across all configured query types. Each query type receives an equal share of the total QPS.
- **Rate Limiter**: Uses `golang.org/x/time/rate` to enforce precise rate limiting. The limiter ensures that the actual query rate matches the configured target QPS.
- **Burst Multiplier**: Allows temporary bursts above the target rate to compensate for network latency and query execution time. The burst size is calculated as `targetQPS * burstMultiplier`.
- **QPS Multiplier**: Optional multiplier applied to the target QPS for compensation scenarios (default: 1.0).

### Time Bucket Distribution

The tool uses a time bucket system to distribute queries across different time ranges, simulating realistic query patterns:

- **Time Buckets**: Defined in configuration with `ageStart` and `ageEnd` parameters representing how far back in time to query.
- **Bucket Eligibility**: Buckets are only used when enough time has elapsed since test start (when `ageEnd <= elapsed time`).
- **Execution Plan**: Each query type has an associated execution plan that specifies which time bucket to use for each query execution.
- **Cycling**: The execution plan cycles through entries in order, repeating when exhausted, ensuring consistent distribution patterns.

### Concurrency Model

- **Concurrent Workers**: Each query type spawns multiple independent worker goroutines (configurable via `concurrentQueries`).
- **Shared Rate Limiter**: All workers for a query type share a single rate limiter, ensuring the total QPS for that query type matches the target.
- **Initial Delay**: Workers start with a small random delay (0-1 second) to spread the initial load and avoid thundering herd effects.

### Query Distribution

- **Equal Distribution**: Total target QPS is divided equally among all configured query types.
- **Per-Query QPS**: Each query type receives `targetQPS / number_of_queries` queries per second.
- **Execution Plan**: The execution plan defines the sequence and time bucket usage for each query type, allowing fine-grained control over query patterns.

### Metrics Collection

The tool exposes Prometheus metrics on port 2112 (`/metrics` endpoint) for monitoring:
- Query latency histograms
- Query failure counters
- Time bucket query counters
- Spans returned histograms

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
  concurrentQueries: 5            # Number of concurrent workers per query type
  targetQPS: 50                   # Total queries per second across all query types
  burstMultiplier: 2.0            # Rate limiter burst multiplier
  qpsMultiplier: 1.0             # Optional QPS compensation multiplier
  limit: 1000                     # Maximum results per query

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
- Creates and starts query executors for each query type
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
- Handles authentication via Bearer tokens
- Supports multi-tenancy via `X-Scope-OrgID` header

**`response.go`**: Defines Tempo API response structures for parsing JSON responses.

#### `internal/generator/`

**`executor.go`**: Manages query execution for a single query type:
- `Executor` - Coordinates workers and rate limiting
- Creates shared rate limiter for all workers of a query type
- Spawns multiple concurrent workers
- Tracks test start time for time bucket eligibility

**`worker.go`**: Individual query execution worker:
- `Worker` - Executes queries in a loop
- Uses builder pattern for flexible construction
- Cycles through execution plan entries
- Selects time buckets based on execution plan
- Records metrics for each query execution
- Handles rate limiting via shared limiter

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
   - Calculate per-query QPS distribution

2. **Executor Creation** (`executor.go`):
   - For each query type, create an `Executor`
   - Create shared rate limiter with target QPS
   - Create Tempo client with authentication

3. **Worker Spawning** (`executor.go`):
   - Spawn `concurrentQueries` workers per query type
   - Each worker starts with random initial delay
   - Workers run concurrently in separate goroutines

4. **Query Execution Loop** (`worker.go`):
   - Worker waits for rate limiter permission
   - Selects next execution plan entry (cycles through entries)
   - Determines time bucket and calculates time range
   - Executes query via Tempo client
   - Records metrics (latency, spans, failures)
   - Repeats loop

5. **Metrics Collection**:
   - All metrics are automatically exposed via Prometheus
   - Available at `http://localhost:2112/metrics`
