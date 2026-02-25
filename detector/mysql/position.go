package mysql

import (
	"fmt"
	"strconv"
	"strings"

	mysqldriver "github.com/go-mysql-org/go-mysql/mysql"
)

// encodePosition encodes a MySQL binlog position (filename + offset) into a
// monotonic uint64 compatible with the existing checkpoint.Store and ack.Tracker:
//
//	uint64 = (file_sequence_number << 32) | uint64(offset)
//
// Example: mysql-bin.000003 offset 4562 -> (3 << 32) | 4562
func encodePosition(pos mysqldriver.Position) (uint64, error) {
	seq, err := parseFileSequence(pos.Name)
	if err != nil {
		return 0, err
	}
	return (uint64(seq) << 32) | uint64(pos.Pos), nil
}

// decodePosition reconstructs a MySQL binlog Position from an encoded uint64.
func decodePosition(encoded uint64, prefix string) mysqldriver.Position {
	seq := uint32(encoded >> 32)
	offset := uint32(encoded & 0xFFFFFFFF)
	return mysqldriver.Position{
		Name: fmt.Sprintf("%s.%06d", prefix, seq),
		Pos:  offset,
	}
}

// parseFileSequence extracts the numeric suffix from a binlog filename.
// Supports "mysql-bin.000003" and "000003" (numeric-only).
func parseFileSequence(filename string) (uint32, error) {
	idx := strings.LastIndex(filename, ".")
	var numPart string
	if idx >= 0 {
		numPart = filename[idx+1:]
	} else {
		numPart = filename
	}

	n, err := strconv.ParseUint(numPart, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse binlog file sequence %q: %w", filename, err)
	}
	return uint32(n), nil
}
