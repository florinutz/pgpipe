package search_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/search"
)

func TestSearchAdapter_Name(t *testing.T) {
	a := search.New("typesense", "http://localhost:8108", "test-key", "test-index", "", 0, 0, 0, 0, nil)
	if got := a.Name(); got != "search" {
		t.Errorf("Name() = %q, want %q", got, "search")
	}
}
