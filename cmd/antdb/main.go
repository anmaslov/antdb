package main

import (
	"antdb/config"
	"antdb/internal/network"
	"antdb/internal/prepare"
	"antdb/internal/service"
	"antdb/internal/service/compute"
	"antdb/internal/service/storage"
	"antdb/internal/service/storage/engine"
	"antdb/internal/service/storage/replication"
	"antdb/internal/service/storage/wal"
	"antdb/internal/tools"
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"
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

	maxSegmentSize, err := tools.ParseSize(cfg.WAL.MaxSegmentSize)
	if err != nil {
		logger.Fatal("can't parse max segment size", zap.Error(err))
	}

	buffer := wal.NewBuffer(cfg.WAL.FlushingBatchSize)
	walWriter := wal.NewWriter(cfg.WAL.DataDirectory, maxSegmentSize, logger)
	walReader := wal.NewReader(cfg.WAL.DataDirectory, logger)
	walJournal := wal.NewWAL(walWriter, walReader, buffer, logger)

	go func() {
		if err = walJournal.Start(ctx, cfg.WAL.FlushingBatchTimeout); err != nil {
			logger.Fatal("can't start wal journal", zap.Error(err))
		}
	}()

	streamCh := make(chan []*wal.Unit)
	var replica replication.Replication
	if cfg.ReplicationConfig.ReplicaType == "master" {
		replica, err = prepare.CreateMasterReplication(cfg.ReplicationConfig, cfg.WAL, logger)
		if err != nil {
			logger.Fatal("can't create master replication", zap.Error(err))
		}
	} else {
		replica, err = prepare.CreateSlaveReplication(cfg.ReplicationConfig, cfg.WAL, streamCh, logger)
		if err != nil {
			logger.Fatal("can't create slave replication", zap.Error(err))
		}
	}
	if cfg.WAL.Compaction {
		logger.Warn("disable replication for compaction")
		replica = nil
	}

	st := storage.NewStorage(engine.NewMemoryTable(), walJournal, replica, walReader.GetStream(), streamCh, logger)
	cmp := compute.NewCompute(compute.NewParser(), compute.NewAnalyzer(logger), logger)
	db := service.NewDatabase(cmp, st, logger)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		if cfg.WAL.Compaction {
			compaction := wal.NewCompaction(cfg.WAL.DataDirectory, time.Second*5, logger)
			err = compaction.Start(ctx)
			if err != nil {
				logger.Fatal("can't start compaction", zap.Error(err))
			}
		} else {
			err = replica.Start(ctx)
			if err != nil {
				logger.Fatal("can't start slave replication", zap.Error(err))
			}
		}
	}()

	go func() {
		defer wg.Done()

		messageSize, err := tools.ParseSize(cfg.Network.MessageSize)
		if err != nil {
			logger.Fatal("can't parse message size", zap.Error(err))
		}

		tcpServer, err := network.NewServer(cfg.Network.Address, cfg.Network.MaxConnections, messageSize, logger)
		if err != nil {
			logger.Fatal("can't create tcp server", zap.Error(err))
		}

		err = tcpServer.Start(ctx, func(ctx context.Context, s []byte) []byte {
			return []byte(db.HandleQuery(ctx, string(s)) + "\n")
		})
		if err != nil {
			logger.Fatal("can't start tcp server", zap.Error(err))
		}

	}()

	wg.Wait()

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
