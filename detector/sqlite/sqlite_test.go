package sqlite

import (
	"database/sql"
	"strings"
	"testing"
)

func TestNew_Defaults(t *testing.T) {
	d := New("/tmp/test.db", 0, 0, false, nil)
	if d.pollInterval != defaultPollInterval {
		t.Errorf("pollInterval = %v, want %v", d.pollInterval, defaultPollInterval)
	}
	if d.batchSize != defaultBatchSize {
		t.Errorf("batchSize = %d, want %d", d.batchSize, defaultBatchSize)
	}
	if d.logger == nil {
		t.Error("logger is nil, want default logger")
	}
	if d.Name() != "sqlite" {
		t.Errorf("Name() = %q, want %q", d.Name(), "sqlite")
	}
}

func TestNew_CustomValues(t *testing.T) {
	d := New("/data/app.db", 2000000000, 50, true, nil)
	if d.pollInterval != 2000000000 {
		t.Errorf("pollInterval = %v, want 2s", d.pollInterval)
	}
	if d.batchSize != 50 {
		t.Errorf("batchSize = %d, want 50", d.batchSize)
	}
	if !d.keepProcessed {
		t.Error("keepProcessed = false, want true")
	}
}

func TestBuildPayload_Insert(t *testing.T) {
	payload := buildPayload("INSERT",
		sql.NullString{String: `{"id":1,"name":"alice"}`, Valid: true},
		sql.NullString{Valid: false},
	)

	s := string(payload)
	if !strings.Contains(s, `"new"`) {
		t.Errorf("payload missing 'new' key: %s", s)
	}
	if strings.Contains(s, `"old"`) {
		t.Errorf("payload should not have 'old' key for INSERT: %s", s)
	}
}

func TestBuildPayload_Update(t *testing.T) {
	payload := buildPayload("UPDATE",
		sql.NullString{String: `{"id":1,"name":"bob"}`, Valid: true},
		sql.NullString{String: `{"id":1,"name":"alice"}`, Valid: true},
	)

	s := string(payload)
	if !strings.Contains(s, `"new"`) {
		t.Errorf("payload missing 'new' key: %s", s)
	}
	if !strings.Contains(s, `"old"`) {
		t.Errorf("payload missing 'old' key: %s", s)
	}
}

func TestBuildPayload_Delete(t *testing.T) {
	payload := buildPayload("DELETE",
		sql.NullString{Valid: false},
		sql.NullString{String: `{"id":1,"name":"alice"}`, Valid: true},
	)

	s := string(payload)
	if strings.Contains(s, `"new"`) {
		t.Errorf("payload should not have 'new' key for DELETE: %s", s)
	}
	if !strings.Contains(s, `"old"`) {
		t.Errorf("payload missing 'old' key: %s", s)
	}
}

func TestBuildPayload_InvalidJSON(t *testing.T) {
	payload := buildPayload("INSERT",
		sql.NullString{String: "not-json", Valid: true},
		sql.NullString{Valid: false},
	)

	s := string(payload)
	if !strings.Contains(s, "not-json") {
		t.Errorf("payload should contain raw string for invalid JSON: %s", s)
	}
}

func TestGenerateInitSQL_SingleTable(t *testing.T) {
	sql := GenerateInitSQL([]string{"users"})

	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS pgcdc_changes") {
		t.Error("missing pgcdc_changes table creation")
	}
	if !strings.Contains(sql, "idx_pgcdc_changes_unprocessed") {
		t.Error("missing index creation")
	}
	if !strings.Contains(sql, "pgcdc_users_insert") {
		t.Error("missing insert trigger for users")
	}
	if !strings.Contains(sql, "pgcdc_users_update") {
		t.Error("missing update trigger for users")
	}
	if !strings.Contains(sql, "pgcdc_users_delete") {
		t.Error("missing delete trigger for users")
	}
}

func TestGenerateInitSQL_MultipleTables(t *testing.T) {
	sql := GenerateInitSQL([]string{"users", "orders", "products"})

	for _, table := range []string{"users", "orders", "products"} {
		if !strings.Contains(sql, "pgcdc_"+table+"_insert") {
			t.Errorf("missing insert trigger for %s", table)
		}
		if !strings.Contains(sql, "pgcdc_"+table+"_update") {
			t.Errorf("missing update trigger for %s", table)
		}
		if !strings.Contains(sql, "pgcdc_"+table+"_delete") {
			t.Errorf("missing delete trigger for %s", table)
		}
	}
}

func TestGenerateInitSQL_EscapesQuotes(t *testing.T) {
	sql := GenerateInitSQL([]string{`my"table`})

	if strings.Contains(sql, `"my"table"`) {
		t.Error("should strip double quotes from table name")
	}
	if !strings.Contains(sql, "pgcdc_mytable_insert") {
		t.Error("trigger name should have quotes stripped")
	}
}
