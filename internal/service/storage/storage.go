package storage

import (
	"antdb/internal/service/compute"
	"antdb/internal/service/storage/replication"
	"antdb/internal/service/storage/wal"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
)

type Storage struct {
	engine      Engine
	wal         *wal.Wal
	replication replication.Replication
	stream      chan []*wal.Unit
	logger      *zap.Logger
}

type Engine interface {
	Set(string, string)
	Get(string) (string, bool)
	Del(string)
}

func NewStorage(engine Engine,
	wal *wal.Wal,
	replication replication.Replication,
	streamInit <-chan []*wal.Unit,
	stream chan []*wal.Unit,
	logger *zap.Logger,
) *Storage {
	storage := &Storage{
		engine:      engine,
		wal:         wal,
		replication: replication,
		stream:      stream,
		logger:      logger,
	}

	// for restore
	for unit := range streamInit {
		storage.applyUnits(unit)
	}

	// for replication
	go func() {
		for unit := range stream {
			storage.applyUnits(unit)
		}
	}()

	return storage
}

func (e *Storage) Set(ctx context.Context, key, value string) error {
	if e.wal != nil {
		if e.replication != nil && !e.replication.IsMaster() {
			return errors.New("can't set in slave")
		}
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
		if e.replication != nil && !e.replication.IsMaster() {
			return errors.New("can't del in slave")
		}
		err := e.wal.Del(ctx, key)
		if err != nil {
			e.logger.Error("error del wal", zap.Error(err))
			return fmt.Errorf("can't del in wal: %w", err)
		}
	}

	e.engine.Del(key)
	return nil
}

func (e *Storage) applyUnits(units []*wal.Unit) {
	for _, unit := range units {
		if unit.Command == string(compute.SetCommand) {
			e.engine.Set(unit.Arguments[0], unit.Arguments[1])
			continue
		}
		if unit.Command == string(compute.DelCommand) {
			e.engine.Del(unit.Arguments[0])
			continue
		}
	}
}
