package network

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSemaphore(t *testing.T) {
	t.Parallel()
	sem := NewSemaphore(2)
	sem.Acquire()
	require.Equal(t, sem.IsFull(), false)

	sem.Acquire()
	require.Equal(t, sem.IsFull(), true)

	sem.Release()
	require.Equal(t, sem.IsFull(), false)
}

func TestSemaphore_WithSemaphore(t *testing.T) {
	t.Parallel()
	sem := NewSemaphore(1)
	sem.WithSemaphore(nil)
	require.Equal(t, sem.IsFull(), false)

	sem.WithSemaphore(func() {
		require.Equal(t, sem.IsFull(), true)
	})
	require.Equal(t, sem.IsFull(), false)
}
