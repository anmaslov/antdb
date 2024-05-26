package wal

import (
	"context"
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"os"
)

type Writer struct {
	directory string

	maxSegmentSize     int
	currentSegmentSize int
	logger             *zap.Logger
}

func NewWriter(dir string, maxSegmentSize int, logger *zap.Logger) *Writer {
	return &Writer{
		directory:      dir,
		maxSegmentSize: maxSegmentSize,
		logger:         logger,
	}
}

func (w *Writer) Flush(_ context.Context, buff *buffer) {
	walBuffer := buff.PopAll()
	if len(walBuffer) == 0 {
		return
	}

	units := make([]*Unit, 0, len(walBuffer))
	for _, unitData := range walBuffer {
		units = append(units, unitData.Unit)
	}
	err := w.Write(units)
	for _, unitData := range walBuffer {
		unitData.ErrChan <- err
	}
}

func (w *Writer) Write(unitsData []*Unit) error {
	file, err := os.OpenFile(w.directory+"wal-test.gob", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("can't open file: %w", err)
	}
	enc := gob.NewEncoder(file)
	err = enc.Encode(&unitsData)
	if err != nil {
		return fmt.Errorf("can't encode data: %w", err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("can't close file: %w", err)
	}

	return nil
}

func (w *Writer) NextSegment() error {
	return nil
}
