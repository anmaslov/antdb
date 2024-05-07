package service

import (
	"antdb/internal/service/compute"
	"antdb/internal/service/storage"
	"context"
	"fmt"
	"go.uber.org/zap"
)

type Database struct {
	compute *compute.Compute
	storage *storage.Engine
	logger  *zap.Logger
}

func NewDatabase(compute *compute.Compute, storage *storage.Engine, logger *zap.Logger) *Database {
	return &Database{
		compute: compute,
		storage: storage,
		logger:  logger,
	}
}

func (d *Database) HandleQuery(ctx context.Context, queryStr string) string {
	d.logger.Debug("handling query", zap.String("query", queryStr))

	query, err := d.compute.HandleQuery(ctx, queryStr)
	if err != nil {
		return fmt.Sprintf("[error] %s", err.Error())
	}

	switch query.GetCommand() {
	case compute.SetCommand:
		d.storage.Set(ctx, query.GetArguments()[0], query.GetArguments()[1])
		return "[OK]"

	case compute.GetCommand:
		val, ok := d.storage.Get(ctx, query.GetArguments()[0])
		if !ok {
			return fmt.Sprintf("[error] not found")
		}
		return fmt.Sprintf("[ok] %s", val)

	case compute.DelCommand:
		d.storage.Del(ctx, query.GetArguments()[0])
		return "[OK]"
	}

	d.logger.Error("can't handle query", zap.String("query", queryStr))
	return "[error] internal error"
}
