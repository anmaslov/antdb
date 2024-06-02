package wal

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestWriter_Write(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	writer := NewWriter(tempDir, 1024, zap.NewNop())

	units := []*Unit{
		{Command: "SET", Arguments: []string{"qwe", "zxc"}},
		{Command: "SET", Arguments: []string{"qwe", "123"}},
		{Command: "SET", Arguments: []string{"asd", "098"}},
	}
	err := writer.Write(units)
	require.NoError(t, err)

	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestWriter_Flush(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	writer := NewWriter(tempDir, 1024, zap.NewNop())
	buff := NewBuffer(1024)
	errCh := buff.Push(context.Background(), &Unit{Command: "SET", Arguments: []string{"qwe", "zxc"}})

	go func() {
		writer.Flush(context.Background(), buff)
	}()

	err := <-errCh
	require.NoError(t, err)

	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, files, 1)

	require.Len(t, buff.PopAll(), 0)
}

func TestWriter_Segment(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	writer := NewWriter(tempDir, 10, zap.NewNop())

	units := []*Unit{
		{Command: "SET", Arguments: []string{"qwe", "zxc"}},
		{Command: "SET", Arguments: []string{"qwe", "123"}},
		{Command: "SET", Arguments: []string{"asd", "09832342342"}},
		{Command: "DEL", Arguments: []string{"qqqqq", "09dsfsdfsd8"}},
	}
	err := writer.Write(units)
	require.NoError(t, err)

	err = writer.Write(units)
	require.NoError(t, err)

	files, err := os.ReadDir(tempDir)
	fmt.Println(len(files))
	require.NoError(t, err)
	require.Len(t, files, 1)
}
