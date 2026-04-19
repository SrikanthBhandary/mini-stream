package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"mini_stream"
	"mini_stream/pb"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"text/template"
	"time"

	"google.golang.org/grpc"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "Address for the HTTP server (e.g., :8080)")
	grpcAddr := flag.String("grpc-addr", ":50051", "Address for the gRPC server (e.g., :50051)")

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// 1. Setup Ingestor
	stream, err := mini_stream.NewIngestor("./data", logger)
	if err != nil {
		log.Fatalf("failed to create ingestor: %v", err)
	}

	// 2. Setup Servers
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStreamServiceServer(grpcServer, &mini_stream.GrpcServer{Ingestor: stream})

	mux := http.NewServeMux()

	// In main.go
	// 1. Initialize templates
	tmpl := template.Must(template.ParseGlob("templates/*.html"))
	web := &mini_stream.WebService{
		Ingestor:  stream,
		Templates: tmpl,
	}

	mux.HandleFunc("GET /", web.ServeDashboard)
	mux.HandleFunc("GET /api/stats-fragment", web.GetStatsFragment)
	mux.HandleFunc("GET /api/topics", web.GetTopics)
	mux.HandleFunc("GET /api/stats", web.GetTotalMessagesInShard)
	mux.HandleFunc("POST /api/topics/create", web.CreateTopic)

	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: mux,
	}

	// 3. Start Servers
	go func() {
		logger.Info("gRPC server starting on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server failed", "error", err)
		}
	}()

	go func() {
		logger.Info("HTTP Server running on :8080")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "error", err)
		}
	}()

	// 4. Graceful Shutdown Logic
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Block until a signal is received
	<-quit
	logger.Info("Shutdown signal received, starting graceful shutdown...")

	// Create a timeout context to ensure shutdown doesn't hang forever
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown HTTP
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP shutdown error", "error", err)
	}

	// Shutdown gRPC
	grpcServer.GracefulStop()

	// Finally, close the Ingestor
	stream.Close()

	logger.Info("Server gracefully stopped")
}
