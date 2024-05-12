package compute

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewQuery(t *testing.T) {
	tests := map[string]struct {
		command   Command
		arguments []string
	}{
		"set command": {
			command:   SetCommand,
			arguments: []string{"key", "value"},
		},
		"get command": {
			command:   GetCommand,
			arguments: []string{"key"},
		},
		"del command": {
			command:   DelCommand,
			arguments: []string{"key"},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			query := NewQuery(test.command, test.arguments)

			require.Equal(t, test.command, query.GetCommand())
			require.Equal(t, test.arguments, query.GetArguments())
		})
	}
}

func TestQueryMapping(t *testing.T) {
	tests := map[string]struct {
		command Command
		word    string
		err     error
	}{
		"set command": {
			command: SetCommand,
			word:    "SET",
			err:     nil,
		},
		"get command": {
			command: GetCommand,
			word:    "GET",
			err:     nil,
		},
		"del command": {
			command: DelCommand,
			word:    "DEL",
			err:     nil,
		},
		"unknown command": {
			command: "",
			word:    "something",
			err:     errInvalidCommand,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			command, err := mapCommand(test.word)

			require.Equal(t, test.err, err)
			require.Equal(t, test.command, command)
		})
	}
}

func TestQueryArgumentNumber(t *testing.T) {
	tests := map[string]struct {
		command Command
		number  int
	}{
		"set command": {
			command: SetCommand,
			number:  3,
		},
		"get command": {
			command: GetCommand,
			number:  2,
		},
		"del command": {
			command: DelCommand,
			number:  2,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			argumentNumber := queryMap[test.command]
			require.Equal(t, test.number, argumentNumber)
		})
	}
}
