package wal

import (
	"antdb/internal/service/compute"
	"context"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type Wal struct {
	walWriter *Writer
	//walReader walReader
	buffer *buffer
	logger *zap.Logger
}

func NewWAL(walWriter *Writer, buffer *buffer, logger *zap.Logger) *Wal {
	return &Wal{
		walWriter: walWriter,
		buffer:    buffer,
		logger:    logger,
	}
}

func (w *Wal) Start(ctx context.Context, timeout time.Duration) error {
	NewWatcher(w.buffer).Watch(ctx, timeout, w.walWriter)
	return nil
}

func (w *Wal) Set(ctx context.Context, key, value string) error {
	errCh := w.buffer.Push(ctx, NewUnit(compute.SetCommand, []string{key, value}))
	if err := <-errCh; err != nil {
		return fmt.Errorf("can't push to buffer: %w", err)
	}

	return nil
}

func (w *Wal) Del(ctx context.Context, key string) error {
	errCh := w.buffer.Push(ctx, NewUnit(compute.DelCommand, []string{key}))
	if err := <-errCh; err != nil {
		return fmt.Errorf("can't push to buffer: %w", err)
	}

	return nil
}
