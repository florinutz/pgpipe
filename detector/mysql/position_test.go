package mysql

import (
	"testing"

	mysqldriver "github.com/go-mysql-org/go-mysql/mysql"
)

func TestEncodeDecodePosition(t *testing.T) {
	tests := []struct {
		name   string
		pos    mysqldriver.Position
		prefix string
	}{
		{
			name:   "typical position",
			pos:    mysqldriver.Position{Name: "mysql-bin.000003", Pos: 4562},
			prefix: "mysql-bin",
		},
		{
			name:   "first file",
			pos:    mysqldriver.Position{Name: "mysql-bin.000001", Pos: 4},
			prefix: "mysql-bin",
		},
		{
			name:   "high offset",
			pos:    mysqldriver.Position{Name: "mysql-bin.000100", Pos: 4294967295},
			prefix: "mysql-bin",
		},
		{
			name:   "zero offset",
			pos:    mysqldriver.Position{Name: "mysql-bin.000001", Pos: 0},
			prefix: "mysql-bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodePosition(tt.pos)
			if err != nil {
				t.Fatalf("encodePosition: %v", err)
			}

			decoded := decodePosition(encoded, tt.prefix)
			if decoded.Name != tt.pos.Name {
				t.Errorf("Name: got %q, want %q", decoded.Name, tt.pos.Name)
			}
			if decoded.Pos != tt.pos.Pos {
				t.Errorf("Pos: got %d, want %d", decoded.Pos, tt.pos.Pos)
			}
		})
	}
}

func TestEncodePositionMonotonic(t *testing.T) {
	pos1 := mysqldriver.Position{Name: "mysql-bin.000003", Pos: 100}
	pos2 := mysqldriver.Position{Name: "mysql-bin.000003", Pos: 200}
	pos3 := mysqldriver.Position{Name: "mysql-bin.000004", Pos: 50}

	e1, _ := encodePosition(pos1)
	e2, _ := encodePosition(pos2)
	e3, _ := encodePosition(pos3)

	if e1 >= e2 {
		t.Errorf("same file, higher offset should be greater: %d >= %d", e1, e2)
	}
	if e2 >= e3 {
		t.Errorf("next file should be greater: %d >= %d", e2, e3)
	}
}

func TestParseFileSequence(t *testing.T) {
	tests := []struct {
		input   string
		want    uint32
		wantErr bool
	}{
		{"mysql-bin.000003", 3, false},
		{"mysql-bin.000001", 1, false},
		{"binlog.000100", 100, false},
		{"000042", 42, false},
		{"mysql-bin.abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseFileSequence(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}
