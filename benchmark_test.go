package mini_stream

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
)

const benchTopic = "bench-topic"

func setupIngestor(b *testing.B) (*Ingestor, func()) {
	b.Helper()
	dir, _ := os.MkdirTemp("", "bench-*")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
	ing, err := NewIngestor(dir, logger)
	if err != nil {
		b.Fatal(err)
	}
	if err := ing.CreateTopic(benchTopic, 4); err != nil {
		b.Fatal(err)
	}
	cleanup := func() {
		ing.Close()
		os.RemoveAll(dir)
	}
	return ing, cleanup
}

// --- Ingest benchmarks ---

func BenchmarkIngest_SmallPayload(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, _, err := ing.Ingest(benchTopic, "user-1", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIngest_LargePayload(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := fmt.Sprintf(`{"data":"%s"}`, string(make([]byte, 1000)))

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, _, err := ing.Ingest(benchTopic, "user-1", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIngest_Parallel(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("user-%d", i%100)
			if _, _, err := ing.Ingest(benchTopic, key, payload); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// --- Read benchmarks ---

func BenchmarkRead_Sequential(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`

	// Pre-populate
	var seqNum uint64
	var shardID int
	for n := 0; n < 10000; n++ {
		seq, shard, _ := ing.Ingest(benchTopic, "user-1", payload)
		seqNum = seq
		shardID = shard
	}
	_ = seqNum
	total := 10000

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		seq := uint64(n % total)
		if _, err := ing.Read(benchTopic, shardID, seq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead_Parallel(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`
	total := 10000

	// Track which shard each key lands on
	shardForKey := make(map[string]int)
	countForShard := make(map[int]int)

	for n := 0; n < total; n++ {
		key := fmt.Sprintf("user-%d", n%100)
		_, shard, _ := ing.Ingest(benchTopic, key, payload)
		shardForKey[key] = shard
		countForShard[shard]++
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			key := fmt.Sprintf("user-%d", n%100)
			shard := shardForKey[key]
			count := countForShard[shard]
			if count == 0 {
				n++
				continue
			}
			ing.Read(benchTopic, shard, uint64(n%count))
			n++
		}
	})
}

// --- Concurrent producer/consumer ---

func BenchmarkProducerConsumer(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`

	// Pre-populate and capture shard
	var shardID int
	for n := 0; n < b.N; n++ {
		_, shard, _ := ing.Ingest(benchTopic, "user-1", payload)
		shardID = shard
	}

	var wg sync.WaitGroup
	wg.Add(2)

	b.ResetTimer()

	// Producer
	go func() {
		defer wg.Done()
		for n := 0; n < b.N; n++ {
			ing.Ingest(benchTopic, "user-1", payload)
		}
	}()

	// Consumer
	go func() {
		defer wg.Done()
		for n := 0; n < b.N; n++ {
			ing.Read(benchTopic, shardID, uint64(n))
		}
	}()

	wg.Wait()
}
