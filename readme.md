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

## Benchmarks
Benchmarked on Apple M5 (ARM64), Go 1.26.

| Benchmark | ns/op | B/op | allocs/op |
| :--- | :--- | :--- | :--- |
| **Ingest (Small)** | 2,305 | 196 | 6 |
| **Ingest (Large)** | 2,946 | 1,173 | 6 |
| **Ingest (Parallel)** | 2,487 | 214 | 7 |
| **Read (Sequential)** | 675 | 96 | 2 |
| **Read (Parallel)** | 1,404 | 104 | 3 |
| **Producer + Consumer** | 5,925 | 286 | 8 |
| **Ingest (128MB Segments)** | 2,267 | 196 | 6 |
| **Ingest (1KB Segments)** | 193,609 | 222 | 6 |

### Performance Analysis
* **The "Rotation Tax":** Our benchmarks demonstrate an ~85x performance penalty when using tiny (1KB) segments vs. large (128MB) segments. This highlights the overhead of file system metadata updates, descriptor allocation, and disk synchronization.
* **Read Scalability:** Sequential read performance (~675ns/op) showcases the efficiency of our log-structured design, which benefits from sequential disk I/O and OS page caching.
* **Concurrency:** The parallel read performance (~1,404ns/op) validates that our lock-free `ReadAt` approach successfully minimizes contention across multiple consumer threads.

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
| **Append-only segments** | Maximizes sequential disk throughput; enables O(1) recovery via simple log replay. |
| **CRC32 per record** | Ensures data integrity; allows detection of partial writes during recovery without global locking. |
| **`ReadAt` (pread)** | Enables concurrent reads without shared file pointers, avoiding mutex bottlenecks on reads. |
| **FD Cache** | Amortizes the cost of `open(2)` syscalls across multiple read operations. |
| **Graceful Shutdown** | Uses `sync.WaitGroup` to flush in-flight buffers, ensuring zero data loss on termination. |

---

## License

MIT

