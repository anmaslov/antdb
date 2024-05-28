package wal

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path"
	"time"
)

type Writer struct {
	directory          string
	file               *os.File
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
	if w.file == nil {
		err := w.createNewSegment()
		if err != nil {
			return fmt.Errorf("can't create new segment: %w", err)
		}
	}

	if w.currentSegmentSize >= w.maxSegmentSize {
		err := w.file.Close()
		if err != nil {
			return fmt.Errorf("can't close file: %w", err)
		}

		err = w.createNewSegment()
		if err != nil {
			return fmt.Errorf("can't create new segment: %w", err)
		}
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(&unitsData)
	if err != nil {
		return fmt.Errorf("can't encode data: %w", err)
	}
	bufSize, err := w.file.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("can't write data: %w", err)
	}
	w.currentSegmentSize += bufSize

	return nil
}

func (w *Writer) createNewSegment() error {
	filename := path.Join(w.directory, fmt.Sprintf("wal-%d.gob", time.Now().Unix()))
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("can't open file: %w", err)
	}
	w.currentSegmentSize = 0
	w.file = file

	return nil
}
