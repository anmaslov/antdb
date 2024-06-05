package wal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path"
	"sort"
	"strings"
)

type Reader struct {
	directory string
	stream    chan []*Unit
	logger    *zap.Logger
}

func NewReader(dir string, logger *zap.Logger) *Reader {
	return &Reader{
		directory: dir,
		stream:    make(chan []*Unit),
		logger:    logger,
	}
}

func (r *Reader) Read() error {
	defer close(r.stream)
	files, err := os.ReadDir(r.directory)
	if err != nil {
		return fmt.Errorf("can't read wal directory: %w", err)
	}

	var segments []string
	for _, file := range files {
		name := file.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".gob") {
			segments = append(segments, name)
		}
	}

	sort.Strings(segments) // asc

	for _, segment := range segments {
		file, err := os.ReadFile(path.Join(r.directory, segment))
		if err != nil {
			return fmt.Errorf("can't open segment [%s]: %w", segment, err)
		}

		datBuf := bytes.NewBuffer(file)
		for datBuf.Len() > 0 {
			var units []*Unit
			decoder := gob.NewDecoder(datBuf)
			if err := decoder.Decode(&units); err != nil {
				return fmt.Errorf("can't parse segment [%s]: %w", segment, err)
			}

			r.stream <- units
		}
	}

	return nil
}

func (r *Reader) GetStream() chan []*Unit {
	return r.stream
}
