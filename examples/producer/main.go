package main

import (
	"context"
	"fmt"
	"log"
	"mini_stream/pb"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewStreamServiceClient(conn)

	fmt.Println("🚀 Producer starting — sending 10 messages...")
	for j := 0; j < 10; j++ {
		payload := fmt.Sprintf(`{"event": "order_placed", "order_id": %d, "user": "user-1"}`, j)

		resp, err := client.Ingest(context.Background(), &pb.IngestRequest{
			Key:     "user-1",
			Payload: payload,
		})
		if err != nil {
			log.Printf("failed to ingest: %v", err)
			continue
		}

		fmt.Printf("✅ Sent   seq=%d  payload=%s\n", resp.SeqNum, payload)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("✅ Producer done.")
}
