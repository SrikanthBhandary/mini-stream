# MiniStream

A production-grade, log-structured message streaming engine built from scratch in Go — inspired by the internals of Apache Kafka. Designed as a deep dive into distributed systems storage primitives: sharding, binary framing, crash recovery, and gRPC transport.

---

## Architecture Overview

```
Producer (gRPC Client)
        │
        ▼
┌──────────────────┐
│   gRPC Server    │  ← Protobuf wire format
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────┐
│            Ingestor Engine           │
│                                      │
│  ┌──────────┐      ┌──────────┐     │
│  │  Shard 0 │      │  Shard 1 │     │  ← CRC32 key routing
│  │          │      │          │     │
│  │ seg-1.log│      │ seg-1.log│     │  ← Segment rotation
│  │ seg-2.log│      │ seg-2.log│     │
│  └──────────┘      └──────────┘     │
│                                      │
│  In-memory offset index (recovered  │
│  on restart via log replay)          │
└──────────────────────────────────────┘
        │
        ▼
Consumer (gRPC Client)
```

---

## Features

- **Sharded storage** — messages routed to shards via CRC32 key hashing for deterministic, even distribution
- **Append-only log segments** — immutable, size-bounded segment files with automatic rotation
- **Binary framing** — length-prefixed records with per-record CRC32 integrity checksums
- **Crash recovery** — full log replay on startup; truncates corrupt or partial records at the crash boundary
- **Concurrent read safety** — uses `ReadAt` (pread syscall) so multiple goroutines read without a shared cursor
- **FD cache** — file descriptors pooled with double-checked locking to avoid repeated `os.Open` costs
- **gRPC transport** — Protobuf-defined `Ingest` and `Read` RPCs with separate producer/consumer CLIs
- **Clean shutdown** — `Close()` drains all open file descriptors safely

---

## On-Disk Record Format

Every record written to a segment file follows this binary layout:

```
┌─────────────┬─────────────┬──────────────────┐
│  Length     │   CRC32     │    Payload        │
│  (4 bytes)  │  (4 bytes)  │   (N bytes)       │
└─────────────┴─────────────┴──────────────────┘
```

On recovery, each record's CRC is verified. A mismatch signals a partial write from a previous crash — the file is truncated at that offset and replay stops.

---

## Project Structure

```
mini_stream/
├── ingestor.go          # Core storage engine
├── grpc_server.go       # gRPC service implementation
├── pb/
│   ├── stream.proto     # Protobuf definitions
│   ├── stream.pb.go     # Generated message types
│   └── stream_grpc.pb.go# Generated service interfaces
├── cmd/
│   ├── server/main.go   # gRPC server entrypoint
│   ├── producer/main.go # Demo producer CLI
│   └── consumer/main.go # Demo consumer CLI
└── data/                # Runtime segment files (gitignored)
    ├── shard-0/
│   │   ├── 1720000000.log
│   │   └── 1720000001.log
    └── shard-1/
        └── 1720000000.log
```

## Benchmarks

Benchmarked on Apple M5 (ARM64, darwin), Go 1.26, 10 GOMAXPROCS.

```
go test -bench=. -benchmem ./...
```

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| Ingest — small payload (~40B) | ~347,000 | 2,882 | 211 | 6 |
| Ingest — large payload (~1KB) | ~64,000 | 15,523 | 1,516 | 13 |
| Ingest — parallel (10 goroutines) | ~373,000 | 2,676 | 228 | 7 |
| Read — sequential | ~1,510,000 | 662 | 96 | 2 |
| Read — parallel (10 goroutines) | ~721,000 | 1,387 | 110 | 3 |
| Producer + Consumer (concurrent) | ~150,000 | 6,630 | 300 | 8 |

### What the numbers mean

**Ingest throughput (~347K msg/sec)** is bottlenecked by the underlying disk write, not by locking. Parallel ingest matches single-threaded ingest because each shard has its own independent write lock — goroutines on different shards never contend.

**Sequential read at 662 ns/op (~1.5M reads/sec)** reflects the FD cache and `ReadAt` working as designed. No `open()` syscall, no seek, just a direct `pread` to the correct offset followed by a CRC32 integrity check.

**Parallel read at 1,387 ns/op (~721K reads/sec)** uses `sync.Map` for lock-free index lookups, bringing parallel read throughput to 2.1x what it was with a standard `RWMutex`-protected map. The remaining overhead vs sequential is `pread` contention at the kernel level on macOS when multiple goroutines read the same file descriptor concurrently.

**Producer + Consumer at 6,630 ns/op** reflects the combined cost of a concurrent write and read to the same shard. In a real workload with multiple keys spread across shards, producers and consumers would be more independent and this number would improve.

### Optimization history

| Change | Read Parallel Before | Read Parallel After | Gain |
|---|---|---|---|
| Baseline | 3,040 ns/op | — | — |
| FD cache moved per-shard, `cacheMu` removed | 3,040 ns/op | 2,929 ns/op | 4% |
| `sync.Map` for `OffsetIndex` (lock-free reads) | 2,929 ns/op | 1,387 ns/op | **2.1x** |

---

## Getting Started

### Prerequisites

- Go 1.21+
- protoc + Go gRPC plugins (only needed if modifying the proto)

```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
```

### Install dependencies

```bash
go mod tidy
```

### Run the demo

Open three terminal windows:

```bash
# Terminal 1 — Start the server
go run cmd/server/main.go

# Terminal 2 — Start the consumer (polls for messages)
go run cmd/consumer/main.go

# Terminal 3 — Fire the producer
go run cmd/producer/main.go
```

**Expected output:**

```
# Producer
🚀 Producer starting — sending 10 messages...
✅ Sent   seq=0  payload={"event": "order_placed", "order_id": 0, "user": "user-1"}
✅ Sent   seq=1  payload={"event": "order_placed", "order_id": 1, "user": "user-1"}
...

# Consumer
👂 Consumer starting — reading from shard 1...
📨 Received seq=0  payload={"event": "order_placed", "order_id": 0, "user": "user-1"}
📨 Received seq=1  payload={"event": "order_placed", "order_id": 1, "user": "user-1"}
...
```

> **Note:** `user-1` hashes to shard 1 via CRC32. The consumer reads from `shard_id: 1` accordingly.

---

## API (Protobuf)

```protobuf
service StreamService {
  rpc Ingest(IngestRequest) returns (IngestResponse);
  rpc Read(ReadRequest)     returns (ReadResponse);
}

message IngestRequest  { string key = 1; string payload = 2; }
message IngestResponse { uint64 seq_num = 1; }
message ReadRequest    { int32 shard_id = 1; uint64 seq_num = 2; }
message ReadResponse   { string payload = 1; }
```

---

## Regenerating Protobuf Files

```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/stream.proto
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Append-only segments | Writes are sequential, maximizing disk throughput and enabling simple crash recovery |
| CRC32 per record | Detects partial writes at the crash boundary without expensive fsync on every write |
| In-memory offset index | O(1) random reads by sequence number; rebuilt from log on restart |
| `ReadAt` over `Seek+Read` | Passes offset directly to `pread(2)` — no shared file cursor, safe for concurrent reads |
| FD cache | Amortizes `open(2)` syscall cost across many reads to the same sealed segment |
| Timestamp-named segments | Lexicographic sort = chronological order, enabling correct replay without metadata files |

---

## What I Learned / Built This To Explore

- How Kafka-style log storage works at the byte level
- Why append-only + offset indexing is a powerful primitive
- The subtleties of safe concurrent file I/O in Go (`ReadAt` vs `Seek`)
- Crash recovery via log replay and CRC-gated truncation
- Composing a gRPC transport layer over a storage engine cleanly

---

## Potential Next Steps

- **Consumer groups** with committed offset tracking persisted to disk
- **Retention / compaction** — TTL-based segment deletion after all groups have consumed past them
- **Replication** — writing each segment to multiple nodes for fault tolerance

---

## License

MIT
