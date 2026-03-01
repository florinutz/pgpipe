package sqlite

import (
	"fmt"
	"strings"
)

// GenerateInitSQL generates the SQL statements needed to set up change tracking
// for the given tables. It creates the pgcdc_changes table and per-table triggers.
func GenerateInitSQL(tables []string) string {
	var b strings.Builder

	b.WriteString(`-- pgcdc change tracking table
CREATE TABLE IF NOT EXISTS pgcdc_changes (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    row_data TEXT,
    old_data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    processed INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pgcdc_changes_unprocessed
ON pgcdc_changes(processed) WHERE processed = 0;

`)

	for _, table := range tables {
		b.WriteString(generateTableTriggers(table))
		b.WriteString("\n")
	}

	return b.String()
}

func generateTableTriggers(table string) string {
	safeName := strings.ReplaceAll(table, "\"", "")
	return fmt.Sprintf(`-- Triggers for table: %s
CREATE TRIGGER IF NOT EXISTS pgcdc_%s_insert
AFTER INSERT ON "%s"
BEGIN
    INSERT INTO pgcdc_changes(table_name, operation, row_data)
    VALUES ('%s', 'INSERT', json(NEW));
END;

CREATE TRIGGER IF NOT EXISTS pgcdc_%s_update
AFTER UPDATE ON "%s"
BEGIN
    INSERT INTO pgcdc_changes(table_name, operation, row_data, old_data)
    VALUES ('%s', 'UPDATE', json(NEW), json(OLD));
END;

CREATE TRIGGER IF NOT EXISTS pgcdc_%s_delete
AFTER DELETE ON "%s"
BEGIN
    INSERT INTO pgcdc_changes(table_name, operation, old_data)
    VALUES ('%s', 'DELETE', json(OLD));
END;
`, safeName,
		safeName, safeName, safeName,
		safeName, safeName, safeName,
		safeName, safeName, safeName)
}
