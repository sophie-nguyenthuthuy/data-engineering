package rpc

import (
	"context"

	"google.golang.org/grpc"
)

// ---- method paths -------------------------------------------------------

const (
	MethodSubmit  = "/compaction.CompactionService/SubmitCompaction"
	MethodStatus  = "/compaction.CompactionService/GetStatus"
	MethodAck     = "/compaction.CompactionService/AcknowledgeCompaction"
	MethodCommit  = "/compaction.CompactionService/CommitCompaction"
)

// ---- server interface ----------------------------------------------------

// CompactionServiceServer is implemented by the remote worker node.
type CompactionServiceServer interface {
	SubmitCompaction(context.Context, *CompactionRequest) (*CompactionResponse, error)
	GetStatus(context.Context, *StatusRequest) (*StatusResponse, error)
	AcknowledgeCompaction(context.Context, *AckRequest) (*AckResponse, error)
	CommitCompaction(context.Context, *CommitRequest) (*CommitResponse, error)
}

// RegisterCompactionServiceServer wires srv into a gRPC server.
func RegisterCompactionServiceServer(s *grpc.Server, srv CompactionServiceServer) {
	s.RegisterService(&compactionServiceDesc, srv)
}

var compactionServiceDesc = grpc.ServiceDesc{
	ServiceName: "compaction.CompactionService",
	HandlerType: (*CompactionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "SubmitCompaction", Handler: handleSubmit},
		{MethodName: "GetStatus", Handler: handleStatus},
		{MethodName: "AcknowledgeCompaction", Handler: handleAck},
		{MethodName: "CommitCompaction", Handler: handleCommit},
	},
}

func handleSubmit(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(CompactionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CompactionServiceServer).SubmitCompaction(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: MethodSubmit},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CompactionServiceServer).SubmitCompaction(ctx, req.(*CompactionRequest))
		})
}

func handleStatus(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(StatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CompactionServiceServer).GetStatus(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: MethodStatus},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CompactionServiceServer).GetStatus(ctx, req.(*StatusRequest))
		})
}

func handleAck(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(AckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CompactionServiceServer).AcknowledgeCompaction(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: MethodAck},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CompactionServiceServer).AcknowledgeCompaction(ctx, req.(*AckRequest))
		})
}

func handleCommit(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(CommitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CompactionServiceServer).CommitCompaction(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: MethodCommit},
		func(ctx context.Context, req any) (any, error) {
			return srv.(CompactionServiceServer).CommitCompaction(ctx, req.(*CommitRequest))
		})
}

// ---- client -------------------------------------------------------------

// CompactionServiceClient calls a remote compaction worker.
type CompactionServiceClient interface {
	SubmitCompaction(ctx context.Context, in *CompactionRequest, opts ...grpc.CallOption) (*CompactionResponse, error)
	GetStatus(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error)
	AcknowledgeCompaction(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error)
	CommitCompaction(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error)
}

type compactionClient struct{ cc grpc.ClientConnInterface }

// NewCompactionServiceClient dials a remote worker and returns a client.
func NewCompactionServiceClient(cc grpc.ClientConnInterface) CompactionServiceClient {
	return &compactionClient{cc}
}

func (c *compactionClient) SubmitCompaction(ctx context.Context, in *CompactionRequest, opts ...grpc.CallOption) (*CompactionResponse, error) {
	out := new(CompactionResponse)
	return out, c.cc.Invoke(ctx, MethodSubmit, in, out, opts...)
}

func (c *compactionClient) GetStatus(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error) {
	out := new(StatusResponse)
	return out, c.cc.Invoke(ctx, MethodStatus, in, out, opts...)
}

func (c *compactionClient) AcknowledgeCompaction(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error) {
	out := new(AckResponse)
	return out, c.cc.Invoke(ctx, MethodAck, in, out, opts...)
}

func (c *compactionClient) CommitCompaction(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error) {
	out := new(CommitResponse)
	return out, c.cc.Invoke(ctx, MethodCommit, in, out, opts...)
}
