package mini_stream

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
)

func setupIngestor(b *testing.B) (*Ingestor, func()) {
	b.Helper()
	dir, _ := os.MkdirTemp("", "bench-*")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // silence logs during bench
	}))
	ing, err := NewIngestor(4, dir, logger)
	if err != nil {
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
		if _, err := ing.Ingest("user-1", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIngest_LargePayload(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	// 1KB payload
	payload := fmt.Sprintf(`{"data":"%s"}`, string(make([]byte, 1000)))

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, err := ing.Ingest("user-1", payload); err != nil {
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
			key := fmt.Sprintf("user-%d", i%100) // spread across shards
			if _, err := ing.Ingest(key, payload); err != nil {
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
	for n := 0; n < 10000; n++ {
		ing.Ingest("user-1", payload)
	}

	// user-1 hashes to a specific shard — find it
	shardID := ing.getShard("user-1")

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		seq := uint64(n % 10000)
		if _, err := ing.Read(shardID, seq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead_Parallel(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`
	total := 10000

	// Populate across multiple shards
	for n := 0; n < total; n++ {
		key := fmt.Sprintf("user-%d", n%100)
		ing.Ingest(key, payload)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			key := fmt.Sprintf("user-%d", n%100)
			shardID := ing.getShard(key) // make getShard public temporarily
			ing.Read(shardID, uint64(n%(total/ing.NumOfShard)))
			n++
		}
	})
}

// --- Concurrent producer/consumer ---

func BenchmarkProducerConsumer(b *testing.B) {
	ing, cleanup := setupIngestor(b)
	defer cleanup()

	payload := `{"event":"order_placed","user":"user-1"}`
	shardID := ing.getShard("user-1")

	// Pre-populate so consumer has something to read
	for n := 0; n < b.N; n++ {
		ing.Ingest("user-1", payload)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	b.ResetTimer()

	// Producer
	go func() {
		defer wg.Done()
		for n := 0; n < b.N; n++ {
			ing.Ingest("user-1", payload)
		}
	}()

	// Consumer
	go func() {
		defer wg.Done()
		for n := 0; n < b.N; n++ {
			ing.Read(shardID, uint64(n))
		}
	}()

	wg.Wait()
}
