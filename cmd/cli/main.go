package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net"
	"os"
	"syscall"
)

func main() {
	address := flag.String("address", ":3223", "db address")
	flag.Parse()

	logger, err := initLogger()
	if err != nil {
		panic(err)
	}

	conn, err := net.Dial("tcp", *address)
	if err != nil {
		logger.Fatal("failed to connect to server", zap.Error(err))
	}
	logger.Info("connected to server")

	defer func() {
		if err = conn.Close(); err != nil {
			logger.Warn("failed to close connection", zap.Error(err))
		}
	}()

	consoleReader := bufio.NewReader(os.Stdin)
	connReader := bufio.NewReader(conn)

	for {
		fmt.Print("[ANTDB] > ")
		command, err := consoleReader.ReadString('\n')
		if err != nil {
			logger.Error("failed to read", zap.Error(err))
		}

		if command == "exit\n" {
			break
		}

		if _, err = conn.Write([]byte(command)); err != nil {
			if errors.Is(err, syscall.EPIPE) {
				logger.Fatal("connection was closed", zap.Error(err))
			}

			logger.Error("failed to send query", zap.Error(err))
			return
		}

		response, err := connReader.ReadString('\n')
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				logger.Fatal("connection was closed", zap.Error(err))
			}

			logger.Error("failed to get response", zap.Error(err))
			return
		}

		fmt.Print(response)
	}
}

func initLogger() (*zap.Logger, error) {
	opts := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
		Development: false,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:     "ts",
			MessageKey:  "msg",
			EncodeTime:  zapcore.ISO8601TimeEncoder, // zapcore.RFC3339TimeEncoder
			EncodeLevel: zapcore.CapitalColorLevelEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	return opts.Build()
}
