package pgtable_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/pgtable"
)

func TestPGTableAdapter_Name(t *testing.T) {
	a := pgtable.New("postgres://test", "", 0, 0, nil)
	if got := a.Name(); got != "pg_table" {
		t.Errorf("Name() = %q, want %q", got, "pg_table")
	}
}
