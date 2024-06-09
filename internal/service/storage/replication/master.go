package replication

import (
	"antdb/internal/service/storage/wal"
	"context"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"os"
	"path"
)

type Server interface {
	Start(context.Context, func(context.Context, []byte) []byte) error
}

type Master struct {
	Server    Server
	directory string
	logger    *zap.Logger
}

func NewMaster(server Server, directory string, logger *zap.Logger) *Master {
	return &Master{
		Server:    server,
		directory: directory,
		logger:    logger,
	}
}

func (m *Master) Start(ctx context.Context) error {
	return m.Server.Start(ctx, func(ctx context.Context, req []byte) []byte {
		segmentReq := &SegmentRequest{}
		if err := proto.Unmarshal(req, segmentReq); err != nil {
			m.logger.Error("failed to unmarshal request", zap.Error(err))
			return nil
		}

		m.logger.Debug("request", zap.String("name", segmentReq.GetLastName().GetValue()))

		response := m.findLastSegment(segmentReq)
		data, err := proto.Marshal(response)
		if err != nil {
			m.logger.Error("failed to marshal response", zap.Error(err))
			return nil
		}

		return data
	})
}

func (m *Master) IsMaster() bool {
	return true
}

func (m *Master) findLastSegment(req *SegmentRequest) *SegmentResponse {
	segment, err := wal.GetNextSegment(m.directory, req.GetLastName().GetValue())
	if err != nil {
		m.logger.Error("failed to get next segments", zap.Error(err))
		return &SegmentResponse{
			Name: &wrapperspb.StringValue{Value: segment},
		}
	}

	if segment == req.GetLastName().GetValue() {
		return &SegmentResponse{
			Name: &wrapperspb.StringValue{Value: segment},
		}
	}

	var data []byte
	if segment != "" {
		data, err = os.ReadFile(path.Join(m.directory, segment))
		if err != nil {
			m.logger.Error("failed to read segment", zap.Error(err))
			return &SegmentResponse{
				Name: &wrapperspb.StringValue{Value: segment},
			}
		}
	}

	return &SegmentResponse{
		Name: &wrapperspb.StringValue{Value: segment},
		Data: data,
	}
}
