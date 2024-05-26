package wal

import (
	"context"
	"sync"
)

type Buffer interface {
	Push(ctx context.Context, value *Unit) chan error
	PopAll() []*UnitData
	GetOversize() <-chan struct{}
}

type buffer struct {
	limit    int
	oversize chan struct{}
	mu       sync.Mutex
	values   []*UnitData
}

// NewBuffer создает буфер
func NewBuffer(limit int) *buffer {
	return &buffer{
		values:   make([]*UnitData, 0, limit),
		limit:    limit,
		oversize: make(chan struct{}, 1),
	}
}

func (b *buffer) Push(ctx context.Context, unit *Unit) chan error {
	errorCh := make(chan error)
	b.mu.Lock()
	defer b.mu.Unlock()

	b.values = append(b.values, &UnitData{Unit: unit, ErrChan: errorCh})

	if len(b.values) >= b.limit && len(b.oversize) == 0 {
		select {
		case b.oversize <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
	}

	return errorCh
}

func (b *buffer) PopAll() []*UnitData {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.values) == 0 {
		return nil
	}

	// нужно ставить len, а не cap, так как: The number of elements copied is the minimum of len(src) and len(dst)
	copyValues := make([]*UnitData, len(b.values))
	copy(copyValues, b.values) // b.values возвращать не можем чтобы не было race condition - поэтому копируем
	b.values = b.values[:0]
	return copyValues
}

func (b *buffer) GetOversize() <-chan struct{} {
	return b.oversize
}
