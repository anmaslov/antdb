package batch_buffer

import (
	"context"
	"sync"
)

type Buffer[T any] interface {
	Push(ctx context.Context, value T)
	PopAll() []T
	GetOversize() <-chan struct{}
}

type buffer[T any] struct {
	limit    int
	oversize chan struct{}

	//mu     sync.Mutex
	values []T
	cond   *sync.Cond
}

// NewBuffer создает буфер
func NewBuffer[T any](limit int) *buffer[T] {
	return &buffer[T]{
		values:   make([]T, 0, limit),
		limit:    limit,
		oversize: make(chan struct{}, 1),
		cond:     sync.NewCond(&sync.Mutex{}),
	}
}

func (b *buffer[T]) Push(ctx context.Context, value T) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	b.values = append(b.values, value)

	if len(b.values) >= b.limit && len(b.oversize) == 0 {
		select {
		case b.oversize <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
	b.cond.Wait()
}

func (b *buffer[T]) PopAll() []T {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	if len(b.values) == 0 {
		return []T{}
	}

	// нужно ставить len, а не cap, так как: The number of elements copied is the minimum of len(src) and len(dst)
	copyValues := make([]T, len(b.values))
	copy(copyValues, b.values) // b.values возвращать не можем чтобы не было race condition - поэтому копируем
	b.values = b.values[:0]
	b.cond.Broadcast()
	return copyValues
}

func (b *buffer[T]) GetOversize() <-chan struct{} {
	return b.oversize
}
