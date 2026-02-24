package ack_test

import (
	"sync"
	"testing"

	"github.com/florinutz/pgcdc/ack"
)

func TestTracker_MinAckedLSN_Empty(t *testing.T) {
	tr := ack.New()
	if got := tr.MinAckedLSN(); got != 0 {
		t.Errorf("empty tracker: want 0, got %d", got)
	}
}

func TestTracker_MinAckedLSN_SingleAdapter(t *testing.T) {
	tr := ack.New()
	tr.Register("stdout", 0)

	if got := tr.MinAckedLSN(); got != 0 {
		t.Errorf("before ack: want 0, got %d", got)
	}

	tr.Ack("stdout", 100)
	if got := tr.MinAckedLSN(); got != 100 {
		t.Errorf("after ack 100: want 100, got %d", got)
	}

	// Lower LSN must not go backwards.
	tr.Ack("stdout", 50)
	if got := tr.MinAckedLSN(); got != 100 {
		t.Errorf("after ack 50 (lower): want 100, got %d", got)
	}

	// Higher LSN advances.
	tr.Ack("stdout", 200)
	if got := tr.MinAckedLSN(); got != 200 {
		t.Errorf("after ack 200: want 200, got %d", got)
	}
}

func TestTracker_MinAckedLSN_MultipleAdapters(t *testing.T) {
	tr := ack.New()
	tr.Register("fast", 0)
	tr.Register("slow", 0)

	tr.Ack("fast", 500)
	// slow has not acked yet; min should be 0.
	if got := tr.MinAckedLSN(); got != 0 {
		t.Errorf("slow at 0: want min 0, got %d", got)
	}

	tr.Ack("slow", 100)
	// min is now slow (100).
	if got := tr.MinAckedLSN(); got != 100 {
		t.Errorf("slow at 100: want min 100, got %d", got)
	}

	tr.Ack("slow", 600)
	// Both at 500 and 600 — min is 500 (fast).
	if got := tr.MinAckedLSN(); got != 500 {
		t.Errorf("slow at 600, fast at 500: want min 500, got %d", got)
	}
}

func TestTracker_ConcurrentAck(t *testing.T) {
	tr := ack.New()
	tr.Register("a", 0)
	tr.Register("b", 0)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(2)
		lsn := uint64(i + 1)
		go func() {
			defer wg.Done()
			tr.Ack("a", lsn)
		}()
		go func() {
			defer wg.Done()
			tr.Ack("b", lsn)
		}()
	}
	wg.Wait()

	got := tr.MinAckedLSN()
	if got != 1000 {
		t.Errorf("after 1000 concurrent acks: want 1000, got %d", got)
	}
}
