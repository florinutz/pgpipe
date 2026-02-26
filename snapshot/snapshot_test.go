package snapshot

import (
	"testing"
)

func TestSetSnapshotName_Valid(t *testing.T) {
	s := New("postgres://...", "users", "", 0, nil)

	tests := []string{
		"00000003-00000029-1",
		"00000003/00000029",
		"AABB00FF",
		"abc-def-123",
	}

	for _, name := range tests {
		if err := s.SetSnapshotName(name); err != nil {
			t.Errorf("SetSnapshotName(%q) returned error: %v", name, err)
		}
	}
}

func TestSetSnapshotName_Invalid(t *testing.T) {
	s := New("postgres://...", "users", "", 0, nil)

	tests := []string{
		"'; DROP TABLE students; --",
		"name with spaces",
		"name\nwith\nnewlines",
		"",
	}

	for _, name := range tests {
		if err := s.SetSnapshotName(name); err == nil {
			t.Errorf("SetSnapshotName(%q) should have returned error", name)
		}
	}
}
