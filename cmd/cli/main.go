package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"net"
	"os"
	"syscall"
)

func main() {
	address := flag.String("address", ":3223", "db address")
	flag.Parse()

	logger, _ := zap.NewProduction()

	conn, err := net.Dial("tcp", *address)
	if err != nil {
		logger.Fatal("failed to connect to server", zap.Error(err))
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			logger.Warn("failed to close connection", zap.Error(err))
		}
	}()

	consoleReader := bufio.NewReader(os.Stdin)
	connReader := bufio.NewReader(conn)

	for {
		fmt.Print("Enter your command: ")
		command, err := consoleReader.ReadString('\n')
		if err != nil {
			logger.Error("failed to read", zap.Error(err))
		}

		_, err = conn.Write([]byte(command))
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				logger.Fatal("connection was closed", zap.Error(err))
			}

			logger.Error("failed to send query", zap.Error(err))
		}

		response, err := connReader.ReadString('\n')
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				logger.Fatal("connection was closed", zap.Error(err))
			}

			logger.Error("failed to get response", zap.Error(err))
		}

		fmt.Println(response)
	}
}
