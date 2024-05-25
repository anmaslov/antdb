package batch_buffer

import (
	"context"
	"time"
)

type Watcher[T any] interface {
	Watch(ctx context.Context, timeout time.Duration, flusher Flusher[T])
}

type watcher[T any] struct {
	buffer Buffer[T]
}

func NewWatcher[T any](buffer Buffer[T]) *watcher[T] {
	return &watcher[T]{buffer}
}

func (w *watcher[T]) Watch(ctx context.Context, timeout time.Duration, flusher Flusher[T]) {
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-w.buffer.GetOversize():
			ticker.Reset(timeout)
			flusher.Flush(ctx, w.buffer)
		case <-ticker.C:
			flusher.Flush(ctx, w.buffer)
		case <-ctx.Done():
			flusher.Flush(ctx, w.buffer)
			return
		}
	}
}
