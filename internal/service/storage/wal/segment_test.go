package wal

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSegment_GetNewerSegmentNames(t *testing.T) {
	tmpDir := "fixtures"

	tests := map[string]struct {
		dir           string
		name          string
		expectedNames []string
		err           error
	}{
		"empty name": {
			dir:           tmpDir,
			name:          "",
			expectedNames: []string{"wal-1716904987.gob", "wal-1716905005.gob", "wal-1716905022.gob"},
			err:           nil,
		},
		"specific name": {
			dir:           tmpDir,
			name:          "wal-1716904987.gob",
			expectedNames: []string{"wal-1716904987.gob", "wal-1716905005.gob", "wal-1716905022.gob"},
			err:           nil,
		},
		"specific name2": {
			dir:           tmpDir,
			name:          "wal-1716905005.gob",
			expectedNames: []string{"wal-1716905005.gob", "wal-1716905022.gob"},
			err:           nil,
		},
		"empty result": {
			dir:           tmpDir,
			name:          "wal-8716905005.gob",
			expectedNames: nil,
			err:           nil,
		},
		"bad dir": {
			dir:           "tmp",
			name:          "wal-8716905005.gob",
			expectedNames: nil,
			err:           errors.New("directory is not exist"),
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res, err := GetNewerSegmentNames(test.dir, test.name)
			if test.err == nil {
				require.NoError(t, err)
			}
			require.Equal(t, test.expectedNames, res)
		})
	}
}

func TestSegment_GetLastSegment(t *testing.T) {
	tmpDir := "fixtures"

	tests := map[string]struct {
		dir          string
		expectedName string
		err          error
	}{
		"get last": {
			dir:          tmpDir,
			expectedName: "wal-1716905022.gob",
			err:          nil,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nextName, err := GetLastSegment(test.dir)
			require.NoError(t, err)
			require.Equal(t, test.expectedName, nextName)
		})
	}

}

func TestSegment_GetNextSegment(t *testing.T) {
	tmpDir := "fixtures"

	tests := map[string]struct {
		dir          string
		name         string
		expectedName string
		err          error
	}{
		"empty name": {
			dir:          tmpDir,
			name:         "",
			expectedName: "wal-1716904987.gob",
			err:          nil,
		},
		"specific name": {
			dir:          tmpDir,
			name:         "wal-1716904987.gob",
			expectedName: "wal-1716905005.gob",
			err:          nil,
		},
		"get last": {
			dir:          tmpDir,
			name:         "wal-1716905022.gob",
			expectedName: "wal-1716905022.gob",
			err:          nil,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nextName, err := GetNextSegment(test.dir, test.name)
			require.NoError(t, err)
			require.Equal(t, test.expectedName, nextName)
		})
	}

}
