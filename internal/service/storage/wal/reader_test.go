package wal

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func TestReader_Read(t *testing.T) {
	tmpdir := "fixtures"
	items := []*Unit{
		{Command: "SET", Arguments: []string{"qwe", "zxc"}},
		{Command: "SET", Arguments: []string{"qwe", "123"}},
		{Command: "SET", Arguments: []string{"asd", "098"}},
		{Command: "SET", Arguments: []string{"zxc", "777"}},
		{Command: "DEL", Arguments: []string{"zxc"}},
	}

	r := NewReader(tmpdir, zap.NewNop())

	go func() {
		if err := r.Read(); err != nil {
			t.Errorf("Read() failed: %v", err)
			return
		}
	}()

	itemCount := 0
	unitsData := make([]*Unit, 0, len(items))
	for units := range r.GetStream() {
		for _, unit := range units {
			unitsData = append(unitsData, unit)
			itemCount++
		}
	}

	require.Equal(t, len(items), itemCount)
	require.True(t, reflect.DeepEqual(items, unitsData))
}
