package wal

import (
	"antdb/internal/service/storage/batch_buffer"
	"context"
	"fmt"
)

type Writer struct {
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Flush(ctx context.Context, buffer batch_buffer.Buffer[Unit]) {
	walBuffer := buffer.PopAll()
	if len(walBuffer) == 0 {
		return
	}

	w.Write(walBuffer)
}

func (w *Writer) Write(units []Unit) error {
	fmt.Println("this is units", units)
	return nil
}
