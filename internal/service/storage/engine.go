package storage

import (
	"context"
	"go.uber.org/zap"
)

type Engine struct {
	storage DbStorage
	logger  *zap.Logger
}

type DbStorage interface {
	Set(string, string)
	Get(string) (string, bool)
	Del(string)
}

func NewEngine(storage DbStorage, logger *zap.Logger) *Engine {
	return &Engine{
		storage: storage,
		logger:  logger,
	}
}

func (e *Engine) Set(_ context.Context, key, value string) {
	e.storage.Set(key, value)
	e.logger.Debug("success set query")
}

func (e *Engine) Get(_ context.Context, key string) (string, bool) {
	value, ok := e.storage.Get(key)
	e.logger.Debug("success get query")
	return value, ok
}

func (e *Engine) Del(_ context.Context, key string) {
	e.storage.Del(key)
	e.logger.Debug("success del query")
}
