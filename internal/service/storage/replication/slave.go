package replication

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"net"
	"time"
)

type Slave struct {
	connection      net.Conn
	syncInterval    time.Duration
	walDirectory    string
	lastSegmentName string
	log             *zap.Logger
}

func NewSlave(address string, syncInterval time.Duration, walDirectory string, log *zap.Logger) (*Slave, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &Slave{
		connection:   connection,
		syncInterval: syncInterval,
		walDirectory: walDirectory,
		log:          log,
	}, nil
}

func (s *Slave) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			resp, err := s.send([]byte("request"))
			if err != nil {
				s.log.Error("failed to send request", zap.Error(err))
				continue
			}
			fmt.Println("response from server:", string(resp))

		case <-ctx.Done():
			return nil
		}
	}

	//return nil
}

func (s *Slave) send(req []byte) ([]byte, error) {
	if _, err := s.connection.Write(req); err != nil {
		return nil, err
	}

	response := make([]byte, 50)
	buf, err := s.connection.Read(response)
	if err != nil {
		return nil, err
	}

	return response[:buf], nil
}
