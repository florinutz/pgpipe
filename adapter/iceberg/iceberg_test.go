package iceberg_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/iceberg"
)

func TestIcebergAdapter_Name(t *testing.T) {
	a := iceberg.New("hadoop", "", "/tmp/warehouse", "default", "test_table", "", "", nil, 0, 0, 0, 0, nil)
	if got := a.Name(); got != "iceberg" {
		t.Errorf("Name() = %q, want %q", got, "iceberg")
	}
}
