package wal

import (
	"antdb/internal/service/compute"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

type Compaction struct {
	directory    string
	isProcessing bool // операция с файлами может быть долгая
	interval     time.Duration
	logger       *zap.Logger
}

func NewCompaction(dir string, interval time.Duration, logger *zap.Logger) *Compaction {
	return &Compaction{
		directory: dir,
		interval:  interval,
		logger:    logger,
	}
}

func (c *Compaction) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if c.isProcessing {
				continue
			}
			err := c.run()
			if err != nil {
				c.logger.Error("can't compact wal", zap.Error(err))
			}
		}
	}
}

func (c *Compaction) run() error {
	c.isProcessing = true
	defer func() {
		c.isProcessing = false
	}()
	files, err := os.ReadDir(c.directory)
	if err != nil {
		return fmt.Errorf("can't read wal directory: %w", err)
	}

	segments := make([]string, 0, len(files))
	for _, file := range files {
		name := file.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".gob") {
			segments = append(segments, name)
		}
	}

	if len(segments) > 1 {
		sort.Strings(segments) // asc
		err = c.compact(segments[:2])
		if err != nil {
			return fmt.Errorf("can't compact segments: %w", err)
		}
	}

	return nil
}

func (c *Compaction) compact(segments []string) error {
	if len(segments) == 0 {
		return nil
	}

	unitsData, err := c.readUnits(segments)
	if err != nil {
		return fmt.Errorf("can't read units: %w", err)
	}
	if len(unitsData) == 0 {
		return nil
	}

	compactedFilename := path.Join(c.directory, fmt.Sprintf("compacted-%d.gob", time.Now().Unix()))
	file, err := os.OpenFile(compactedFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("can't open file: %w", err)
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err = encoder.Encode(&unitsData)
	if err != nil {
		return fmt.Errorf("can't encode data: %w", err)
	}
	_, err = file.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("can't write data: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("can't sync file: %w", err)
	}

	for _, segment := range segments {
		err = os.Remove(path.Join(c.directory, segment))
		if err != nil {
			return fmt.Errorf("can't remove segment [%s]: %w", segment, err)
		}
	}

	err = os.Rename(compactedFilename, path.Join(c.directory, segments[0]))
	if err != nil {
		return fmt.Errorf("can't rename file segment: %w", err)
	}

	return nil
}

func (c *Compaction) readUnits(segments []string) ([]*Unit, error) {
	memoryTable := make(map[string]string)
	for _, segment := range segments {
		file, err := os.ReadFile(path.Join(c.directory, segment))
		if err != nil {
			return nil, fmt.Errorf("can't open segment [%s]: %w", segment, err)
		}

		datBuf := bytes.NewBuffer(file)
		for datBuf.Len() > 0 {
			var units []*Unit
			decoder := gob.NewDecoder(datBuf)
			if err := decoder.Decode(&units); err != nil {
				return nil, fmt.Errorf("can't parse segment [%s]: %w", segment, err)
			}

			for _, unit := range units {
				if unit.Command == string(compute.SetCommand) {
					memoryTable[unit.Arguments[0]] = unit.Arguments[1]
					continue
				}
				if unit.Command == string(compute.DelCommand) {
					delete(memoryTable, unit.Arguments[0])
					continue
				}
			}
		}
	}

	var unitsData []*Unit
	for key, val := range memoryTable {
		unitsData = append(unitsData, NewUnit(compute.SetCommand, []string{key, val}))
	}

	return unitsData, nil
}
