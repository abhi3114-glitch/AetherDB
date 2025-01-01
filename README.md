# AetherDB

A high-performance time-series database (TSDB) optimized for metrics storage and querying, inspired by Prometheus and InfluxDB.

## Features

- **Append-Only Storage**: Write-ahead log (WAL) for durability with in-memory buffer for fast writes
- **Block Compaction**: Tiered compaction system (2h, 6h, 24h blocks) for efficient storage
- **Downsampling**: Pre-computed aggregates at 1-minute, 5-minute, and 1-hour resolutions
- **Query Engine**: Support for sum, avg, max, min, delta, and rate aggregations
- **REST API**: HTTP endpoints for writing and querying data
- **CLI Tool**: Command-line interface for database management

## Quick Start

### Build

```powershell
cd c:\PROJECTS\AetherDB
go mod tidy
go build -o aetherdb.exe ./cmd/aetherdb
go build -o aetherctl.exe ./cmd/aetherctl
```

### Run the Server

```powershell
.\aetherdb.exe --data-dir=./data --port=9090
```

### Write Data

Using curl:

```bash
# Write a single metric
curl -X POST http://localhost:9090/api/v1/write -d "cpu_usage,host=server1 45.2"

# Write multiple metrics
curl -X POST http://localhost:9090/api/v1/write -d "cpu_usage,host=server1,region=us-east value=45.2
memory_used,host=server1 value=8589934592
disk_io,host=server1,device=sda value=1024"
```

Using the CLI:

```powershell
echo "cpu_usage,host=server1 45.2" | .\aetherctl.exe write
```

### Query Data

```bash
# Instant query
curl "http://localhost:9090/api/v1/query?query=avg(cpu_usage[5m])"

# Range query
curl "http://localhost:9090/api/v1/query_range?query=cpu_usage&start=1699500000&end=1699510000&step=60"
```

Using the CLI:

```powershell
.\aetherctl.exe query "sum(cpu_usage{host=\"server1\"}[1h])"
```

## Architecture

```
Write Path:
  [Incoming Metrics] -> [WAL] -> [MemTable] -> [Block Storage]

Background Jobs:
  [Compaction: L0->L1->L2]  [Downsampling: 1m, 5m, 1h]

Read Path:
  [Query Parser] -> [MemTable + Blocks] -> [Aggregation]
```

### Core Components

| Component | Description |
|-----------|-------------|
| WAL | Append-only write-ahead log with CRC checksums for crash recovery |
| MemTable | In-memory buffer for recent data with fast queries |
| Block | Immutable, compressed on-disk storage using delta-of-delta encoding |
| Compactor | Merges smaller blocks into larger ones at configurable levels |
| Downsampler | Pre-computes aggregates (min, max, sum, count) at multiple resolutions |
| Query Engine | Parses queries and applies aggregation functions |

## Data Model

### Series

A series is identified by a metric name and a set of labels:

```
cpu_usage{host="server1", region="us-east"}
```

### Sample

A sample is a (timestamp, value) pair:

```
timestamp: 1699500000000  (Unix milliseconds)
value: 45.2               (float64)
```

### Line Protocol

Write data using line protocol format:

```
metric_name,label1=value1,label2=value2 value timestamp
```

Examples:

```
cpu_usage,host=server1 45.2
memory_used,host=server1,region=us-east 8589934592 1699500000000
```

## Query Language

### Basic Query

```
metric_name{label1="value1", label2=~"regex.*"}[duration]
```

### Aggregation Functions

| Function | Description |
|----------|-------------|
| sum() | Sum of all values |
| avg() | Average of all values |
| max() | Maximum value |
| min() | Minimum value |
| delta() | Difference between last and first value |
| rate() | Per-second rate of change |
| count() | Number of samples |

### Examples

```
# Average CPU usage over the last 5 minutes
avg(cpu_usage[5m])

# Sum of requests with specific labels
sum(http_requests{method="GET", status="200"}[1h])

# Rate of network bytes
rate(network_bytes_total[5m])
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| /api/v1/write | POST | Write samples (line protocol) |
| /api/v1/query | GET/POST | Execute instant query |
| /api/v1/query_range | GET | Execute range query |
| /api/v1/series | GET | List all series |
| /api/v1/status | GET | Database status |
| /health | GET | Health check |
| /api/v1/admin/flush | POST | Force memtable flush |
| /api/v1/admin/compact | POST | Trigger compaction |
| /api/v1/admin/downsample | POST | Trigger downsampling |

## CLI Commands

```powershell
# Show database status
.\aetherctl.exe status

# Execute a query
.\aetherctl.exe query "avg(cpu_usage[5m])"

# Write data from stdin
echo "cpu_usage,host=server1 45.2" | .\aetherctl.exe write

# Force flush
.\aetherctl.exe flush

# Trigger compaction
.\aetherctl.exe compact

# Trigger downsampling
.\aetherctl.exe downsample
```

## Testing

Run all tests:

```powershell
go test ./... -v
```

Run benchmarks:

```powershell
go test ./internal/storage -bench=. -benchmem
```

## Project Structure

```
AetherDB/
├── cmd/
│   ├── aetherdb/          # Main database server
│   └── aetherctl/         # CLI tool
├── internal/
│   ├── api/               # REST API server
│   ├── downsample/        # Downsampling engine
│   ├── model/             # Data models
│   ├── query/             # Query engine
│   └── storage/           # Storage engine
├── go.mod
└── README.md
```

## Performance Targets

- Write Throughput: >100K samples/second
- Query Latency: <100ms for 1M sample range
- Compression: ~1.5 bytes/sample (delta-of-delta encoding)
- Durability: Zero data loss with WAL

## Technical Details

### Storage Format

The database uses a LSM-tree inspired architecture:

1. Writes go to WAL first for durability
2. Data accumulates in MemTable (in-memory)
3. When MemTable reaches size limit, it flushes to immutable blocks
4. Background compaction merges blocks at different levels

### Compression

Timestamps use delta-of-delta encoding:
- First timestamp: stored as 64-bit integer
- Subsequent timestamps: store difference from expected delta
- Variable-length encoding for small deltas

This achieves significant compression for regularly-spaced metrics.

### Downsampling

The database automatically creates downsampled versions of data:
- 1-minute resolution: min, max, sum, count, first, last
- 5-minute resolution: aggregated from raw or 1m data
- 1-hour resolution: aggregated from 5m data

Queries automatically select the appropriate resolution based on time range.

## License

MIT License
