package compute

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestParser_Parse(t *testing.T) {
	tests := map[string]struct {
		query  string
		tokens []string
		err    error
	}{
		"empty query": {
			query: "",
		},
		"query without tokens": {
			query: "   ",
			err:   ErrParse,
		},
		"query with UTF symbols": {
			query: "字文下",
			err:   ErrInvalidSymbol,
		},
		"query with one token": {
			query:  "set",
			tokens: []string{"set"},
		},
		"query with two tokens": {
			query:  "set key",
			tokens: []string{"set", "key"},
		},
		"query with one token with digits": {
			query:  "2set1",
			tokens: []string{"2set1"},
		},
		"query with one token with underscores": {
			query:  "_set__",
			tokens: []string{"_set__"},
		},
		"query with one token with invalid symbols": {
			query: ".set#",
			err:   ErrInvalidSymbol,
		},
		"query with two tokens with additional spaces": {
			query:  "set   key  ",
			tokens: []string{"set", "key"},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			parser := NewParser()

			tokens, err := parser.Parse(test.query)
			require.Equal(t, test.err, err)
			require.True(t, reflect.DeepEqual(test.tokens, tokens))
		})
	}
}
