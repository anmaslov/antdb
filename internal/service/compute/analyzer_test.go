package compute

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func TestAnalyzer_Analyze(t *testing.T) {
	tests := map[string]struct {
		tokens []string
		query  *Query
		err    error
	}{
		"empty tokens": {
			tokens: []string{},
			err:    errInvalidCommand,
		},
		"invalid command": {
			tokens: []string{"TRUNCATE"},
			err:    errInvalidCommand,
		},
		"invalid number arguments for set query": {
			tokens: []string{"SET", "key"},
			err:    errInvalidArguments,
		},
		"invalid number arguments for get query": {
			tokens: []string{"GET", "key", "value"},
			err:    errInvalidArguments,
		},
		"invalid number arguments for del query": {
			tokens: []string{"GET", "key", "value"},
			err:    errInvalidArguments,
		},
		"valid set query": {
			tokens: []string{"SET", "key", "value"},
			query:  NewQuery(SetCommand, []string{"key", "value"}),
		},
		"valid get query": {
			tokens: []string{"GET", "key"},
			query:  NewQuery(GetCommand, []string{"key"}),
		},
		"valid del query": {
			tokens: []string{"DEL", "key"},
			query:  NewQuery(DelCommand, []string{"key"}),
		},
	}

	analyzer := NewAnalyzer(zap.NewNop())

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			query, err := analyzer.Analyze(test.tokens)
			require.Equal(t, test.query, query)
			require.Equal(t, test.err, err)
		})
	}
}
