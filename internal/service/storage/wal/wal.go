package wal

import (
	"antdb/internal/service/compute"
	"antdb/internal/service/storage/batch_buffer"
	"context"
	"go.uber.org/zap"
	"time"
)

type Unit struct {
	command   compute.Command
	arguments []string
}

type Wal struct {
	walWriter *Writer
	//walReader walReader
	buffer batch_buffer.Buffer[Unit]

	logger *zap.Logger
}

func NewWAL(walWriter *Writer, buffer batch_buffer.Buffer[Unit], logger *zap.Logger) *Wal {
	return &Wal{
		walWriter: walWriter,
		buffer:    buffer,
		logger:    logger,
	}
}

func (w *Wal) Start(ctx context.Context, timeout time.Duration) error {
	w.logger.Info("todo start watcher")
	watcher := batch_buffer.NewWatcher(w.buffer)
	watcher.Watch(ctx, timeout, w.walWriter)
	return nil
}

func (w *Wal) Set(ctx context.Context, key, value string) error {
	w.buffer.Push(ctx, Unit{
		command:   compute.SetCommand,
		arguments: []string{key, value},
	})

	return nil
}

func (w *Wal) Del(ctx context.Context, key string) error {
	w.buffer.Push(ctx, Unit{
		command:   compute.DelCommand,
		arguments: []string{key},
	})

	return nil
}
