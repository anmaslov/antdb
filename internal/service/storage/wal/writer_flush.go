package wal

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Writer struct {
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Flush(_ context.Context, buff *buffer) {
	walBuffer := buff.PopAll()
	if len(walBuffer) == 0 {
		return
	}

	err := w.Write(walBuffer)
	for _, unitData := range walBuffer {
		unitData.ErrChan <- err
	}
}

func (w *Writer) Write(unitsData []*UnitData) error {
	fmt.Println("prepare to sleep")
	time.Sleep(10 * time.Second)
	for _, unitData := range unitsData {
		fmt.Println("this is unit", unitData.Unit)
	}

	return errors.New("cho to wse ne tak")
}
