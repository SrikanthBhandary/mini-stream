package main

import (
	"context"
	"fmt"
	"log/slog"
	"mini_stream"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	stream, err := mini_stream.NewIngestor(2, "./data", logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1) // add this

	}

	// g handles the WaitGroup and context for us
	g, _ := errgroup.WithContext(context.Background())

	// PRODUCER
	g.Go(func() error {
		for j := 0; j < 10; j++ {
			stream.Ingest("user-1", fmt.Sprintf(`{"val": %d}`, j))
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	// CONSUMER
	g.Go(func() error {
		var next uint64
		for next < 10 {
			val, err := stream.Read(0, next)
			if err != nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			fmt.Println("Consumed:", val)
			next++
		}
		return nil
	})

	// Wait() blocks until all functions in g.Go return
	if err := g.Wait(); err != nil {
		fmt.Printf("Error in stream: %v\n", err)
	}
}
