package compute

import (
	"errors"
)

type Command string

const (
	SetCommand Command = "SET"
	GetCommand Command = "GET"
	DelCommand Command = "DEL"
)

const (
	setArgumentsNumber = 3
	getArgumentsNumber = 2
	delArgumentsNumber = 2
)

var (
	errInvalidCommand   = errors.New("invalid command")
	errInvalidArguments = errors.New("invalid arguments")
)

var commandMap = map[string]Command{
	"SET": SetCommand,
	"GET": GetCommand,
	"DEL": DelCommand,
}

var queryMap = map[Command]int{
	SetCommand: setArgumentsNumber,
	GetCommand: getArgumentsNumber,
	DelCommand: delArgumentsNumber,
}

type Query struct {
	command   Command
	arguments []string
}

func NewQuery(command Command, arguments []string) *Query {
	return &Query{
		command:   command,
		arguments: arguments,
	}
}

func (q *Query) GetCommand() Command {
	return q.command
}

func (q *Query) GetArguments() []string {
	return q.arguments
}

func mapCommand(word string) (Command, error) {
	command, ok := commandMap[word]
	if !ok {
		return "", errInvalidCommand
	}

	return command, nil
}
