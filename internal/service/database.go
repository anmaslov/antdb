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
	storage *storage.Storage
	logger  *zap.Logger
}

func NewDatabase(compute *compute.Compute, storage *storage.Storage, logger *zap.Logger) *Database {
	return &Database{
		compute: compute,
		storage: storage,
		logger:  logger,
	}
}

type QueryHandler interface {
	HandleQuery(ctx context.Context, queryStr string) string
}

func (d *Database) HandleQuery(ctx context.Context, queryStr string) string {
	d.logger.Debug("handling query", zap.String("query", queryStr))

	query, err := d.compute.HandleQuery(ctx, queryStr)
	if err != nil {
		return fmt.Sprintf("[error] %s", err.Error())
	}

	switch query.GetCommand() {
	case compute.SetCommand:
		return d.handleSet(ctx, query)
	case compute.GetCommand:
		return d.handleGet(ctx, query)
	case compute.DelCommand:
		return d.handleDel(ctx, query)
	}

	d.logger.Error("can't handle query", zap.String("query", queryStr))
	return "[error] internal error"
}

func (d *Database) handleSet(ctx context.Context, query *compute.Query) string {
	err := d.storage.Set(ctx, query.GetArguments()[0], query.GetArguments()[1])
	if err != nil {
		return fmt.Sprintf("[error] %s", err.Error())
	}

	return "[ok]"
}

func (d *Database) handleGet(ctx context.Context, query *compute.Query) string {
	val, err := d.storage.Get(ctx, query.GetArguments()[0])
	if err != nil {
		return fmt.Sprintf("[error] %s", err.Error())
	}
	return fmt.Sprintf("[ok] %s", val)
}

func (d *Database) handleDel(ctx context.Context, query *compute.Query) string {
	if err := d.storage.Del(ctx, query.GetArguments()[0]); err != nil {
		return fmt.Sprintf("[error] %s", err.Error())
	}

	return "[ok]"
}
