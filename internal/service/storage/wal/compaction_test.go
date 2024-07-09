package wal

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"path"
	"reflect"
	"testing"
	"time"
)

func prepareTempDir(t *testing.T, tempDir string) {
	fixturesDir := "fixtures"
	files, err := os.ReadDir(fixturesDir)
	require.NoError(t, err)

	for _, file := range files {
		fileBytes, err := os.ReadFile(path.Join(fixturesDir, file.Name()))
		require.NoError(t, err)

		err = os.WriteFile(path.Join(tempDir, file.Name()), fileBytes, 0o644)
		require.NoError(t, err)
	}
}

func TestReadUnits(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)
	prepareTempDir(t, tempDir)

	compaction := NewCompaction(tempDir, time.Second, zap.NewNop())

	segments := []string{"wal-1716904987.gob", "wal-1716905005.gob", "wal-1716905022.gob"}
	units, err := compaction.readUnits(segments)
	require.NoError(t, err)
	require.Equal(t, 2, len(units))

	expectedUnits := []*Unit{
		{Command: "SET", Arguments: []string{"qwe", "123"}},
		{Command: "SET", Arguments: []string{"asd", "098"}},
	}
	require.True(t, reflect.DeepEqual(expectedUnits, units))
}

func TestCompaction_compact(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)
	prepareTempDir(t, tempDir)

	compaction := NewCompaction(tempDir, time.Second, zap.NewNop())
	segments := []string{"wal-1716904987.gob", "wal-1716905005.gob", "wal-1716905022.gob"}

	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Equal(t, 3, len(files))

	err = compaction.compact(segments)
	require.NoError(t, err)

	files, err = os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, "wal-1716904987.gob", files[0].Name())
}
