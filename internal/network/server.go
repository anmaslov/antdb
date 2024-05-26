package network

import (
	"bufio"
	"context"
	"errors"
	"go.uber.org/zap"
	"net"
)

type Server struct {
	address   string
	semaphore *Semaphore
	logger    *zap.Logger
}

type TCPHandler = func(context.Context, string) string

func NewServer(address string, maxConnectionsNumber int, logger *zap.Logger) (*Server, error) {
	if maxConnectionsNumber < 1 {
		return nil, errors.New("invalid max connections")
	}

	return &Server{
		address:   address,
		semaphore: NewSemaphore(maxConnectionsNumber),
		logger:    logger,
	}, nil
}

func (s *Server) Start(ctx context.Context, handler TCPHandler) error {
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

			go func(connection net.Conn) {
				s.semaphore.WithSemaphore(func() {
					s.handleConnection(ctx, connection, handler)
				})
			}(conn)
		}
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn, handler TCPHandler) {
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Warn("failed to close connection", zap.Error(err))
		}
	}()

	if handler == nil {
		return
	}

	reader := bufio.NewReader(conn)
	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			s.logger.Warn("can't read command", zap.Error(err))
			return
		}

		if _, err = conn.Write([]byte(handler(ctx, command) + "\n")); err != nil {
			s.logger.Warn("can't write response", zap.Error(err))
		}
	}
}
