package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"mini_stream/pb"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const topic = "orders"

func main() {
	shard := flag.Int("shard", 0, "shard to consume from")
	count := flag.Int("count", 10, "number of messages to consume")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewStreamServiceClient(conn)

	fmt.Printf("👂 Consumer starting — topic=%s shard=%d\n", topic, *shard)

	var next uint64
	for {
		resp, err := client.Read(context.Background(), &pb.ReadRequest{
			Topic:   topic,
			ShardId: int32(*shard),
			SeqNum:  next,
		})
		if err != nil {
			fmt.Printf("❌ ERROR: Failed to read seq=%d: %v\n", next, err)
			fmt.Printf("⏳ Waiting for seq=%d...\n", next)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		fmt.Printf("📨 Received seq=%d  payload=%s\n", next, resp.Payload)
		next++

		if next >= uint64(*count) {
			break
		}
	}

	fmt.Println("✅ Consumer done.")
}
