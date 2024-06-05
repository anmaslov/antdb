package wal

import (
	"context"
	"time"
)

type Watcher interface {
	Watch(ctx context.Context, timeout time.Duration, flusher Flusher)
}

type watcher struct {
	buffer *buffer
}

func NewWatcher(buff *buffer) *watcher {
	return &watcher{buff}
}

func (w *watcher) Watch(ctx context.Context, timeout time.Duration, flusher Flusher) {
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
