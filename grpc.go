package mini_stream

import (
	"context"
	"log"
	"mini_stream/pb"
)

type GrpcServer struct {
	pb.UnimplementedStreamServiceServer
	Ingestor *Ingestor
}

func (s *GrpcServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	seqNum, shardID, err := s.Ingestor.Ingest(req.Topic, req.Key, req.Payload)
	if err != nil {
		return nil, err
	}
	return &pb.IngestResponse{SeqNum: seqNum, ShardId: int32(shardID)}, nil
}

func (s *GrpcServer) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("DEBUG: Received Read request for topic=%s, shard=%d, seq=%d", req.Topic, req.ShardId, req.SeqNum)
	payload, err := s.Ingestor.Read(req.Topic, int(req.ShardId), req.SeqNum)
	if err != nil {
		log.Printf("DEBUG: Read failed: %v", err) // Log the specific error
		return nil, err
	}
	return &pb.ReadResponse{Payload: payload}, nil
}

func (s *GrpcServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := s.Ingestor.CreateTopic(req.Topic, int(req.NumShards))
	if err != nil {
		return &pb.CreateTopicResponse{Success: false}, err
	}
	return &pb.CreateTopicResponse{Success: true}, nil
}
