package network

import (
	"antdb/internal/service"
	"bufio"
	"context"
	"errors"
	"go.uber.org/zap"
	"net"
)

type Server struct {
	address string
	logger  *zap.Logger
}

func NewServer(address string, maxConnectionsNumber int, logger *zap.Logger) (*Server, error) {
	if maxConnectionsNumber < 1 {
		return nil, errors.New("invalid max connections")
	}

	return &Server{
		address: address,
		logger:  logger,
	}, nil
}

func (s *Server) Start(ctx context.Context, handler service.QueryHandler) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return errors.New("can't start server")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Error("can't accept connection", zap.Error(err))
			}

			go s.handleConnection(ctx, conn, handler)
		}
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn, handler service.QueryHandler) {
	defer func() {
		err := conn.Close()
		if err != nil {
			s.logger.Warn("failed to close connection", zap.Error(err))
		}
	}()

	reader := bufio.NewReader(conn)
	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			s.logger.Warn("can't read command", zap.Error(err))
			return
		}
		_, err = conn.Write([]byte(handler.HandleQuery(ctx, command) + "\n"))
		if err != nil {
			s.logger.Warn("can't write response", zap.Error(err))
		}
	}
}
