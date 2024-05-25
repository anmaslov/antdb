package storage

import (
	"antdb/internal/service/storage/wal"
	"context"
	"fmt"
	"go.uber.org/zap"
)

type Storage struct {
	engine Engine
	wal    *wal.Wal
	logger *zap.Logger
}

type Engine interface {
	Set(string, string)
	Get(string) (string, bool)
	Del(string)
}

func NewStorage(engine Engine, wal *wal.Wal, logger *zap.Logger) *Storage {
	return &Storage{
		engine: engine,
		wal:    wal,
		logger: logger,
	}
}

func (e *Storage) Set(ctx context.Context, key, value string) error {
	if e.wal != nil {
		err := e.wal.Set(ctx, key, value)
		if err != nil {
			e.logger.Error("error set in wal", zap.Error(err))
			return fmt.Errorf("can't set in wal: %w", err)
		}
	}

	e.engine.Set(key, value)
	return nil
}

func (e *Storage) Get(_ context.Context, key string) (string, error) {
	value, ok := e.engine.Get(key)
	if !ok {
		return "", fmt.Errorf("not found")
	}

	return value, nil
}

func (e *Storage) Del(ctx context.Context, key string) error {

	if e.wal != nil {
		err := e.wal.Del(ctx, key)
		if err != nil {
			e.logger.Error("error del wal", zap.Error(err))
			return fmt.Errorf("can't del in wal: %w", err)
		}
	}

	e.engine.Del(key)
	return nil
}
