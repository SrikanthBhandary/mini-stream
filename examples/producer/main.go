package main

import (
	"context"
	"fmt"
	"log"
	"mini_stream/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	topic = "orders"
	key   = "user-1"
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

	// Create topic first (idempotent — server returns error if exists, we ignore it)
	_, err = client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:     topic,
		NumShards: 3,
	})
	if err != nil {
		log.Printf("CreateTopic: %v (may already exist)", err)
	}

	fmt.Println("🚀 Producer starting — sending 10 messages...")

	for j := 0; j < 1000; j++ {
		payload := fmt.Sprintf(`{"event":"order_placed","order_id":%d,"user":"user-1"}`, j)

		resp, err := client.Ingest(context.Background(), &pb.IngestRequest{
			Topic:   topic,
			Key:     key,
			Payload: payload,
		})
		if err != nil {
			log.Printf("failed to ingest: %v", err)
			continue
		}

		fmt.Printf("✅ Sent   seq=%d  shard=%d  payload=%s\n", resp.SeqNum, resp.ShardId, payload)
		// time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("✅ Producer done.")
}
