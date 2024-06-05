package wal

import (
	"antdb/internal/service/compute"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNewUnit(t *testing.T) {
	tests := map[string]struct {
		command   compute.Command
		arguments []string
		want      *Unit
	}{
		"SET command": {
			command:   compute.SetCommand,
			arguments: []string{"key", "val"},
			want:      &Unit{Command: "SET", Arguments: []string{"key", "val"}},
		},
		"DEL command": {
			command:   compute.DelCommand,
			arguments: []string{"key"},
			want:      &Unit{Command: "DEL", Arguments: []string{"key"}},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.True(t, reflect.DeepEqual(test.want, NewUnit(test.command, test.arguments)))
		})
	}
}
