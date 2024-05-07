package main

import (
	"antdb/config"
	"antdb/internal/service"
	"antdb/internal/service/compute"
	"antdb/internal/service/storage"
	"bufio"
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"os/signal"
	"sync"
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

	cmp := compute.NewCompute(compute.NewParser(), compute.NewAnalyzer(logger), logger)
	st := storage.NewEngine(storage.NewMemoryTable(), logger)
	db := service.NewDatabase(cmp, st, logger)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		startServer(ctx, db)
	}()

	logger.Debug("started server")
	wg.Wait()
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

func startServer(ctx context.Context, db *service.Database) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if !scanner.Scan() {
				return
			}

			fmt.Println(db.HandleQuery(ctx, scanner.Text()))
		}
	}
}
