package network

import (
	"bufio"
	"context"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"net"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := NewServer(":3223", 10, 1024, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, server)

	go func() {
		err = server.Start(ctx, func(ctx context.Context, s []byte) []byte {
			return []byte("ok\n")
		})
		require.NoError(t, err)
	}()

	// Ждем, пока сервер стартует
	time.Sleep(100 * time.Millisecond)

	connection, err := net.Dial("tcp", ":3223")
	require.NoError(t, err)
	_, err = connection.Write([]byte("send\n"))
	require.NoError(t, err)

	connReader := bufio.NewReader(connection)
	response, err := connReader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ok\n", response)
	err = connection.Close()
	require.NoError(t, err)
}
