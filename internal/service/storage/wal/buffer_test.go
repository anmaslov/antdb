package wal

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewBuffer(t *testing.T) {
	limit := 10
	b := NewBuffer(limit)

	require.NotNil(t, b)
	require.Len(t, b.values, 0)
	require.Equal(t, limit, b.limit)
	require.NotNil(t, b.oversize)
}

func TestBufferPush(t *testing.T) {
	b := NewBuffer(2)
	ctx := context.Background()

	errCh1 := b.Push(ctx, &Unit{})
	require.NotNil(t, errCh1)
	require.Len(t, b.values, 1)

	errCh2 := b.Push(ctx, &Unit{})
	require.NotNil(t, errCh2)
	require.Len(t, b.values, 2)

	select {
	case <-b.GetOversize():
	default:
		t.Error("Expected oversize signal")
	}

	errCh3 := b.Push(ctx, &Unit{})
	require.NotNil(t, errCh3)
	require.Len(t, b.values, 3)
}

func TestBufferPopAll(t *testing.T) {
	b := NewBuffer(3)
	ctx := context.Background()

	b.Push(ctx, &Unit{})
	b.Push(ctx, &Unit{})
	b.Push(ctx, &Unit{})

	values := b.PopAll()
	require.Len(t, values, 3)
	require.Len(t, b.values, 0)

	values = b.PopAll()
	require.Nil(t, values)
}
