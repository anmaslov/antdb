package replication

import (
	"context"
	"fmt"
)

type Server interface {
	Start(context.Context, func(context.Context, []byte) []byte) error
}

type Master struct {
	Server Server
}

func NewMaster(server Server) *Master {
	return &Master{
		Server: server,
	}
}

func (m *Master) Start(ctx context.Context) error {
	return m.Server.Start(ctx, func(ctx context.Context, req []byte) []byte {
		fmt.Println("request ", req)
		return []byte("OK")
	})
}
