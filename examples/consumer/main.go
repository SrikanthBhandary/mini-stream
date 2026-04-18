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

	fmt.Println("👂 Consumer starting — reading from shard 0...")

	var next uint64
	for {
		resp, err := client.Read(context.Background(), &pb.ReadRequest{
			ShardId: 0,
			SeqNum:  next,
		})
		if err != nil {
			// Not yet available — poll
			fmt.Printf("⏳ Waiting for seq=%d...\n", next)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		fmt.Printf("📨 Received seq=%d  payload=%s\n", next, resp.Payload)
		next++

		// Stop after 10 messages for the demo
		if next >= 10 {
			break
		}
	}

	fmt.Println("✅ Consumer done.")
}
