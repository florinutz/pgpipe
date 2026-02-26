package view

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		emit      EmitMode
		wantErr   bool
		wantItems int
		wantGroup []string
		wantWin   time.Duration
	}{
		{
			name:      "count with group by",
			query:     "SELECT COUNT(*) as order_count, payload.region FROM pgcdc_events WHERE channel = 'pgcdc:orders' GROUP BY payload.region TUMBLING WINDOW 1m",
			wantItems: 2,
			wantGroup: []string{"payload.region"},
			wantWin:   time.Minute,
		},
		{
			name:      "global aggregation no group by",
			query:     "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 30s",
			wantItems: 1,
			wantWin:   30 * time.Second,
		},
		{
			name:      "multiple aggregates",
			query:     "SELECT COUNT(*) as cnt, SUM(payload.amount) as total, AVG(payload.amount) as avg_amount FROM pgcdc_events WHERE operation = 'INSERT' GROUP BY payload.region TUMBLING WINDOW 5m",
			wantItems: 3,
			wantGroup: []string{"payload.region"},
			wantWin:   5 * time.Minute,
		},
		{
			name:      "having clause",
			query:     "SELECT COUNT(*) as cnt FROM pgcdc_events GROUP BY payload.region HAVING COUNT(*) > 5 TUMBLING WINDOW 1m",
			wantItems: 1,
			wantGroup: []string{"payload.region"},
			wantWin:   time.Minute,
		},
		{
			name:      "batch emit",
			query:     "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 10s",
			emit:      EmitBatch,
			wantItems: 1,
			wantWin:   10 * time.Second,
		},
		{
			name:    "missing tumbling window",
			query:   "SELECT COUNT(*) FROM pgcdc_events",
			wantErr: true,
		},
		{
			name:    "wrong table",
			query:   "SELECT COUNT(*) FROM other_table TUMBLING WINDOW 1m",
			wantErr: true,
		},
		{
			name:    "invalid duration",
			query:   "SELECT COUNT(*) FROM pgcdc_events TUMBLING WINDOW xyz",
			wantErr: true,
		},
		{
			name:    "not a select",
			query:   "INSERT INTO pgcdc_events VALUES (1) TUMBLING WINDOW 1m",
			wantErr: true,
		},
		{
			name:      "min and max",
			query:     "SELECT MIN(payload.price) as min_price, MAX(payload.price) as max_price FROM pgcdc_events TUMBLING WINDOW 1m",
			wantItems: 2,
			wantWin:   time.Minute,
		},
		{
			name:      "AND/OR WHERE",
			query:     "SELECT COUNT(*) as n FROM pgcdc_events WHERE channel = 'pgcdc:orders' AND (operation = 'INSERT' OR operation = 'UPDATE') TUMBLING WINDOW 1m",
			wantItems: 1,
			wantWin:   time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, err := Parse("test_view", tt.query, tt.emit, 0)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(def.SelectItems) != tt.wantItems {
				t.Errorf("select items = %d, want %d", len(def.SelectItems), tt.wantItems)
			}

			if len(def.GroupBy) != len(tt.wantGroup) {
				t.Errorf("group by = %v, want %v", def.GroupBy, tt.wantGroup)
			} else {
				for i, g := range def.GroupBy {
					if g != tt.wantGroup[i] {
						t.Errorf("group by[%d] = %q, want %q", i, g, tt.wantGroup[i])
					}
				}
			}

			if def.WindowSize != tt.wantWin {
				t.Errorf("window = %v, want %v", def.WindowSize, tt.wantWin)
			}

			if def.FromTable != "pgcdc_events" {
				t.Errorf("from = %q, want pgcdc_events", def.FromTable)
			}

			if def.Emit != tt.emit {
				t.Errorf("emit = %d, want %d", def.Emit, tt.emit)
			}
		})
	}
}

func TestParse_SelectAliases(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as order_count, SUM(payload.amount) as total_revenue FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	if def.SelectItems[0].Alias != "order_count" {
		t.Errorf("alias[0] = %q, want order_count", def.SelectItems[0].Alias)
	}
	if def.SelectItems[1].Alias != "total_revenue" {
		t.Errorf("alias[1] = %q, want total_revenue", def.SelectItems[1].Alias)
	}
}

func TestExtractWindow(t *testing.T) {
	tests := []struct {
		input   string
		wantDur time.Duration
		wantErr bool
	}{
		{"SELECT * FROM t TUMBLING WINDOW 1m", time.Minute, false},
		{"SELECT * FROM t TUMBLING WINDOW 30s", 30 * time.Second, false},
		{"SELECT * FROM t TUMBLING WINDOW 5m", 5 * time.Minute, false},
		{"SELECT * FROM t tumbling window 1h", time.Hour, false},
		{"SELECT * FROM t", 0, true},
		{"SELECT * FROM t TUMBLING WINDOW -1s", 0, true},
	}

	for _, tt := range tests {
		dur, _, err := extractWindow(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("extractWindow(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("extractWindow(%q): %v", tt.input, err)
			continue
		}
		if dur != tt.wantDur {
			t.Errorf("extractWindow(%q) = %v, want %v", tt.input, dur, tt.wantDur)
		}
	}
}
