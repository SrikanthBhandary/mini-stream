# MiniStream

A production-grade, log-structured message streaming engine built from scratch in Go. Inspired by the internals of Apache Kafka, MiniStream is a deep dive into distributed systems storage primitives: sharding, binary framing, crash recovery, and gRPC transport.

## Architecture Overview

```text
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
│  In-memory offset index (recovered   │
│  on restart via log replay)          │
└──────────────────────────────────────┘
        │
        ▼
Consumer (gRPC Client)
```

## Features

- **Sharded storage** — messages routed to shards via CRC32 key hashing for deterministic distribution.
- **Append-only log segments** — immutable, size-bounded files with automatic rotation.
- **Binary framing** — length-prefixed records with per-record CRC32 integrity checksums.
- **Crash recovery** — full log replay on startup; truncates corrupt/partial records at the crash boundary.
- **Concurrent read safety** — uses `ReadAt` (pread syscall) to enable lock-free reads without shared cursors.
- **FD cache** — file descriptors pooled with double-checked locking to avoid `os.Open` overhead.
- **gRPC transport** — Protobuf-defined service with separate producer/consumer CLIs.
- **Graceful Shutdown** — uses `sync.WaitGroup` to gate file closures, ensuring zero data loss during process termination.

---

## On-Disk Record Format

Every record written to a segment file follows this binary layout:

```text
┌─────────────┬─────────────┬──────────────────┐
│  Length     │   CRC32     │    Payload       │
│  (4 bytes)  │  (4 bytes)  │   (N bytes)      │
└─────────────┴─────────────┴──────────────────┘
```

---

## Benchmarks

Benchmarked on Apple M5 (ARM64), Go 1.26, 10 GOMAXPROCS.

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
| :--- | :--- | :--- | :--- | :--- |
| **Ingest (Small Payload)** | ~5,200 | 191,956 | 222 | 6 |
| **Ingest (Large Payload)** | ~250 | 4,055,861 | 1,574 | 12 |
| **Ingest (Parallel)** | ~5,200 | 190,992 | 223 | 7 |
| **Read (Sequential)** | ~1,450,000 | 687 | 96 | 2 |
| **Read (Parallel)** | ~700,000 | 1,458 | 104 | 3 |
| **Producer + Consumer** | ~4,300 | 236,692 | 321 | 8 |

### Performance Analysis
* **Durability:** Ingest throughput numbers reflect a "safe" configuration where every write is durability-guaranteed via `ActiveFile.Sync()`. The bottleneck is disk I/O synchronization, ensuring no data is lost during a crash.
* **Read Scalability:** Sequential reads are near-memory speed. The parallel read performance demonstrates that our `sync.Map` approach effectively eliminates lock contention.

---

## Getting Started

### Prerequisites
- Go 1.21+
- protoc + Go gRPC plugins

### Run the demo

```bash
# Terminal 1 — Start the server
go run cmd/server/main.go

# Terminal 2 — Start the consumer
go run cmd/consumer/main.go

# Terminal 3 — Fire the producer
go run cmd/producer/main.go
```

---

## Key Design Decisions

| Decision | Rationale |
| :--- | :--- |
| **Append-only segments** | Writes are sequential, maximizing disk throughput and enabling simple crash recovery. |
| **CRC32 per record** | Detects partial writes at the crash boundary without heavy global locking. |
| **In-memory index** | Enables O(1) random reads by sequence number; rebuilt from log on restart. |
| **`ReadAt`** | Passes offset directly to `pread(2)` — no shared file cursor, safe for concurrent reads. |
| **Graceful Shutdown** | `sync.WaitGroup` prevents file handles from closing while disk writes are in-flight. |
| **FD Cache** | Amortizes `open(2)` syscall cost across many reads to sealed segments. |

---

## License

MIT
