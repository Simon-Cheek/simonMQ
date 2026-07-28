package wal

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

func segmentFileName(firstLSN uint64) string {
	return fmt.Sprintf("%020d.wal", firstLSN)
}

func listSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") {
			segments = append(segments, entry.Name())
		}
	}
	sort.Strings(segments)
	return segments, nil
}
