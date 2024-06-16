package wal

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

func GetNewerSegmentNames(dir string, name string) ([]string, error) {
	parseName := func(fileName string) (int64, error) {
		if fileName == "" {
			return 0, nil
		}
		strName := strings.TrimSuffix(strings.TrimPrefix(fileName, "wal-"), ".gob")
		fileTimestamp, err := strconv.ParseInt(strName, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("can't parse timestamp: %w", err)
		}
		return fileTimestamp, nil
	}

	fileTimestamp, err := parseName(name)
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("can't read directory: %w", err)
	}

	var segments []string
	for _, file := range files {
		timestamp, err := parseName(file.Name())
		if err != nil {
			continue
		}

		if timestamp >= fileTimestamp {
			segments = append(segments, file.Name())
		}
	}
	sort.Strings(segments)

	return segments, nil
}

func GetLastSegment(dir string) (string, error) {
	segments, err := GetNewerSegmentNames(dir, "")
	if err != nil {
		return "", err
	}
	if len(segments) == 0 {
		return "", nil
	}
	return segments[len(segments)-1], nil
}

func GetNextSegment(dir string, name string) (string, error) {
	segments, err := GetNewerSegmentNames(dir, name)
	if err != nil {
		return "", err
	}
	if len(segments) == 0 {
		return "", nil
	}
	if len(segments) == 1 || name == "" {
		return segments[0], nil
	}

	return segments[1], nil
}
