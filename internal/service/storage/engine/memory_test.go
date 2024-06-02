package engine

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMemoryTable_Set(t *testing.T) {
	t.Run("should set a key-value pair", func(t *testing.T) {
		table := NewMemoryTable()
		table.Set("key1", "value1")
		value, found := table.Get("key1")
		require.True(t, found)
		require.Equal(t, "value1", value)
	})

	t.Run("should overwrite an existing key", func(t *testing.T) {
		table := NewMemoryTable()
		table.Set("key1", "value1")
		table.Set("key1", "value2")
		value, found := table.Get("key1")
		require.True(t, found)
		require.Equal(t, "value2", value)
	})
}

func TestMemoryTable_Get(t *testing.T) {
	t.Run("should return the value and true if the key exists", func(t *testing.T) {
		table := NewMemoryTable()
		table.Set("key1", "value1")
		value, found := table.Get("key1")
		require.True(t, found)
		require.Equal(t, "value1", value)
	})

	t.Run("should return an empty string and false if the key does not exist", func(t *testing.T) {
		table := NewMemoryTable()
		value, found := table.Get("key1")
		require.False(t, found)
		require.Empty(t, value)
	})
}

func TestMemoryTable_Del(t *testing.T) {
	t.Run("should delete the key-value pair", func(t *testing.T) {
		table := NewMemoryTable()
		table.Set("key1", "value1")
		table.Del("key1")
		value, found := table.Get("key1")
		require.False(t, found)
		require.Empty(t, value)
	})

	t.Run("should not return an error if the key does not exist", func(t *testing.T) {
		table := NewMemoryTable()
		table.Del("key1")
		value, found := table.Get("key1")
		require.False(t, found)
		require.Empty(t, value)
	})
}
