package main

import (
	"log"
	"log/slog"
	"mini_stream"
	"mini_stream/pb"
	"net"
	"os"

	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	stream, err := mini_stream.NewIngestor("./data", logger)
	stream.SetFileSizeLimit(1024 * 1024 * 1024) // todo: improve get this by config
	if err != nil {
		log.Fatalf("failed to create ingestor: %v", err)
	}
	defer stream.Close()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStreamServiceServer(grpcServer, &mini_stream.GrpcServer{Ingestor: stream})

	logger.Info("gRPC server starting on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
