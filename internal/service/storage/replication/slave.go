package replication

import (
	"antdb/internal/service/storage/wal"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"net"
	"os"
	"path"
	"time"
)

type Slave struct {
	connection      net.Conn
	syncInterval    time.Duration
	walDirectory    string
	lastSegmentName string
	stream          chan<- []*wal.Unit
	log             *zap.Logger
}

func NewSlave(
	address string,
	syncInterval time.Duration,
	walDirectory string,
	stream chan<- []*wal.Unit,
	log *zap.Logger,
) (*Slave, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	lastSegment, err := wal.GetLastSegment(walDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to get last segment: %w", err)
	}

	return &Slave{
		connection:      connection,
		syncInterval:    syncInterval,
		walDirectory:    walDirectory,
		lastSegmentName: lastSegment,
		stream:          stream,
		log:             log,
	}, nil
}

func (s *Slave) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sync(ctx)

		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Slave) IsMaster() bool {
	return false
}

func (s *Slave) sync(ctx context.Context) {
	req := &SegmentRequest{
		LastName: &wrapperspb.StringValue{Value: s.lastSegmentName},
	}
	s.log.Debug("request", zap.Any("request", req))
	data, err := proto.Marshal(req)
	if err != nil {
		s.log.Error("failed to marshal request", zap.Error(err))
		return
	}

	resp, err := s.send(data)
	if err != nil {
		s.log.Error("failed to send request", zap.Error(err))
		return
	}

	segmentResponse := &SegmentResponse{}
	err = proto.Unmarshal(resp, segmentResponse)
	if err != nil {
		s.log.Error("failed to unmarshal response", zap.Error(err))
		return
	}

	err = s.saveSegment(segmentResponse.GetName().GetValue(), segmentResponse.GetData())
	if err != nil {
		s.log.Error("failed to save segment", zap.Error(err))
		return
	}

	err = s.applyDataToEngine(segmentResponse.GetData())
	if err != nil {
		s.log.Error("failed to apply data to engine", zap.Error(err))
		return
	}

	s.lastSegmentName = segmentResponse.GetName().GetValue()
}

func (s *Slave) send(req []byte) ([]byte, error) {
	if _, err := s.connection.Write(req); err != nil {
		return nil, err
	}

	response := make([]byte, 10<<20)
	buf, err := s.connection.Read(response)
	if err != nil {
		return nil, err
	}

	return response[:buf], nil
}

func (s *Slave) saveSegment(name string, data []byte) error {
	if name == "" {
		return nil
	}
	filename := path.Join(s.walDirectory, name)
	segment, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create segment: %w", err)
	}

	if _, err = segment.Write(data); err != nil {
		return fmt.Errorf("failed to write segment: %w", err)
	}

	return segment.Sync()
}

func (s *Slave) applyDataToEngine(segmentData []byte) error {
	if len(segmentData) == 0 {
		return nil
	}

	var units []*wal.Unit
	buffer := bytes.NewBuffer(segmentData)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&units); err != nil {
		return fmt.Errorf("failed to decode data: %w", err)
	}

	s.stream <- units
	return nil
}
