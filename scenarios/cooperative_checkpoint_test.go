//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pgcdc "github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// TestScenario_CooperativeCheckpoint verifies that cooperative checkpointing
// constrains WAL checkpoint advancement to the minimum acked LSN across all
// adapters.
func TestScenario_CooperativeCheckpoint(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "coop_happy_orders"
		pubName := "pgcdc_coop_happy"
		slotName := "pgcdc_coop_happy_slot"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)
		t.Cleanup(func() { dropReplicationSlot(t, connStr, slotName) })

		capture := newLineCapture()
		logger := testLogger()

		store, err := checkpoint.NewPGStore(context.Background(), connStr, logger)
		if err != nil {
			t.Fatalf("create checkpoint store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetPersistentSlot(slotName)
		walDet.SetCheckpointStore(store)

		stdoutAdapter := stdout.New(capture, logger)

		p := pgcdc.NewPipeline(walDet,
			pgcdc.WithAdapter(stdoutAdapter),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithBusMode(bus.BusModeReliable),
			pgcdc.WithCooperativeCheckpoint(true),
			pgcdc.WithCheckpointStore(store),
			pgcdc.WithLogger(logger),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		pipelineErr := make(chan error, 1)
		go func() { pipelineErr <- p.Run(ctx) }()

		// Wait for slot setup and replication to start.
		time.Sleep(3 * time.Second)

		// Insert rows and verify events arrive with LSN set.
		for i := range 3 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("order_%d", i)})
		}

		for i := range 3 {
			line := capture.waitLine(t, 10*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("event %d: invalid JSON: %v\nraw: %s", i, err, line)
			}
			if ev.Operation != "INSERT" {
				t.Errorf("event %d: expected INSERT, got %q", i, ev.Operation)
			}
		}

		// Wait for standby status interval + some buffer.
		time.Sleep(12 * time.Second)

		// Checkpoint should have advanced: stdout acked all events.
		lsn := queryCheckpoint(t, connStr, slotName)
		if lsn == 0 {
			t.Error("cooperative checkpoint: expected checkpoint LSN > 0, got 0")
		}

		// Checkpoint LSN must not exceed current WAL position.
		currentLSN := queryCurrentWALLSN(t, connStr)
		if lsn > currentLSN {
			t.Errorf("checkpoint LSN %d exceeds current WAL LSN %d", lsn, currentLSN)
		}

		cancel()
		select {
		case err := <-pipelineErr:
			_ = err // context.Canceled on clean shutdown
		case <-time.After(5 * time.Second):
			t.Error("pipeline did not shut down in time")
		}
	})

	t.Run("slow adapter delays checkpoint", func(t *testing.T) {
		table := "coop_slow_orders"
		pubName := "pgcdc_coop_slow"
		slotName := "pgcdc_coop_slow_slot"

		createTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)
		t.Cleanup(func() { dropReplicationSlot(t, connStr, slotName) })

		logger := testLogger()

		store, err := checkpoint.NewPGStore(context.Background(), connStr, logger)
		if err != nil {
			t.Fatalf("create checkpoint store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		walDet := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		walDet.SetPersistentSlot(slotName)
		walDet.SetCheckpointStore(store)

		capture := newLineCapture()
		fastAdapter := stdout.New(capture, logger)

		// slowAdapter consumes events but withholds acking until holdCh is closed.
		holdCh := make(chan struct{})
		slowA := &slowAckAdapter{
			name:   "slow",
			holdCh: holdCh,
		}

		p := pgcdc.NewPipeline(walDet,
			pgcdc.WithAdapter(fastAdapter),
			pgcdc.WithAdapter(slowA),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithBusMode(bus.BusModeReliable),
			pgcdc.WithCooperativeCheckpoint(true),
			pgcdc.WithCheckpointStore(store),
			pgcdc.WithLogger(logger),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		pipelineErr := make(chan error, 1)
		go func() { pipelineErr <- p.Run(ctx) }()

		time.Sleep(3 * time.Second)

		// Insert rows.
		for i := range 3 {
			insertRow(t, connStr, table, map[string]any{"item": fmt.Sprintf("slow_%d", i)})
		}

		// Wait for fast adapter to receive all events.
		for range 3 {
			capture.waitLine(t, 10*time.Second)
		}

		// Wait past standby interval; slow adapter hasn't acked yet.
		time.Sleep(12 * time.Second)
		lsnBefore := queryCheckpoint(t, connStr, slotName)

		// Release slow adapter.
		close(holdCh)

		// Wait for another standby interval.
		time.Sleep(12 * time.Second)
		lsnAfter := queryCheckpoint(t, connStr, slotName)

		// After release: checkpoint should have advanced.
		if lsnAfter <= lsnBefore {
			t.Errorf("checkpoint did not advance after slow adapter was released: before=%d after=%d", lsnBefore, lsnAfter)
		}
		if lsnAfter == 0 {
			t.Error("checkpoint LSN is still 0 after slow adapter was released")
		}

		cancel()
		select {
		case err := <-pipelineErr:
			_ = err
		case <-time.After(5 * time.Second):
			t.Error("pipeline did not shut down in time")
		}
	})
}

// slowAckAdapter implements adapter.Adapter and adapter.Acknowledger.
// It consumes events from its channel but withholds calling ackFn until
// holdCh is closed.
type slowAckAdapter struct {
	name   string
	ackFn  adapter.AckFunc
	holdCh chan struct{}
}

func (a *slowAckAdapter) Name() string                  { return a.name }
func (a *slowAckAdapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

func (a *slowAckAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			// Block until holdCh is closed (or context cancelled).
			select {
			case <-a.holdCh:
			case <-ctx.Done():
				return ctx.Err()
			}
			// Ack after being released.
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}

// queryCurrentWALLSN returns the current WAL write position as a uint64.
func queryCurrentWALLSN(t *testing.T, connStr string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("queryCurrentWALLSN connect: %v", err)
	}
	defer conn.Close(ctx)

	var lsnStr string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsnStr); err != nil {
		t.Fatalf("queryCurrentWALLSN: %v", err)
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		t.Fatalf("queryCurrentWALLSN parse: %v", err)
	}
	return uint64(lsn)
}
