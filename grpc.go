package mini_stream

import (
	"context"
	"mini_stream/pb"
)

type GrpcServer struct {
	pb.UnimplementedStreamServiceServer
	Ingestor *Ingestor
}

func (s *GrpcServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	seqNum, err := s.Ingestor.Ingest(req.Key, req.Payload)
	if err != nil {
		return nil, err
	}
	return &pb.IngestResponse{SeqNum: seqNum}, nil
}

func (s *GrpcServer) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	payload, err := s.Ingestor.Read(int(req.ShardId), req.SeqNum)
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Payload: payload}, nil
}
