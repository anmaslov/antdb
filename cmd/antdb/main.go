package main

import (
	"antdb/config"
	"antdb/internal/network"
	"antdb/internal/service"
	"antdb/internal/service/compute"
	"antdb/internal/service/storage"
	"antdb/internal/service/storage/batch_buffer"
	"antdb/internal/service/storage/engine"
	"antdb/internal/service/storage/wal"
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.GetConfig()
	if err != nil {
		log.Fatal("can't get config", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	logger, err := initLogger(cfg.Logging)
	if err != nil {
		log.Fatal("can't init logger", err)
	}

	buffer := batch_buffer.NewBuffer[wal.Unit](cfg.WAL.FlushingBatchSize)
	walJournal := wal.NewWAL(wal.NewWriter(), buffer, logger)
	// если путь до wal файла не задан - не подключаем
	if cfg.WAL.DataDirectory != "-" {
		go func() {
			if err = walJournal.Start(ctx, cfg.WAL.FlushingBatchTimeout); err != nil {
				logger.Fatal("can't start wal journal", zap.Error(err))
			}
		}()
	} else {
		walJournal = nil
	}

	st := storage.NewStorage(engine.NewMemoryTable(), walJournal, logger)
	cmp := compute.NewCompute(compute.NewParser(), compute.NewAnalyzer(logger), logger)
	db := service.NewDatabase(cmp, st, logger)

	tcpServer, err := network.NewServer(cfg.Network.Address, cfg.Network.MaxConnections, logger)
	if err != nil {
		logger.Fatal("can't create tcp server", zap.Error(err))
	}

	err = tcpServer.Start(ctx, func(ctx context.Context, s string) string {
		return db.HandleQuery(ctx, s)
	})
	if err != nil {
		logger.Fatal("can't start tcp server", zap.Error(err))
	}

	logger.Debug("shutdown server")
}

func initLogger(logCfg *config.LoggingConfig) (*zap.Logger, error) {
	lvl := zap.InfoLevel
	err := lvl.UnmarshalText([]byte(logCfg.Level))
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal log-level: %s", err)
	}

	opts := zap.NewProductionConfig()
	opts.Level = zap.NewAtomicLevelAt(lvl)
	opts.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	if logCfg.Output == "console" {
		opts.Encoding = "console"
		opts.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		opts.OutputPaths = []string{logCfg.Output}
	}

	return opts.Build()
}
