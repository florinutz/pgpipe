// Package ack provides cooperative checkpointing for WAL adapters.
// Adapters report the highest LSN they have successfully processed; the
// Tracker exposes MinAckedLSN() so the WAL detector can advance the
// standby status only as far as the slowest adapter has confirmed.
package ack

import (
	"sync"
	"sync/atomic"

	"github.com/florinutz/pgcdc/metrics"
)

// adapterPos holds a per-adapter atomic position for lock-free Ack calls.
type adapterPos struct {
	lsn  atomic.Uint64
	name string
}

// Tracker tracks the highest acknowledged LSN per adapter and computes the
// minimum across all registered adapters for cooperative checkpointing.
//
// Ack() is lock-free: it loads the per-adapter atomic pointer from a sync.Map
// (optimized for read-heavy workloads) and does an atomic compare-and-swap.
// Register/Deregister use a mutex for safe map mutation.
// MinAckedLSN iterates all adapters (called infrequently, every ~10s).
type Tracker struct {
	mu       sync.Mutex   // protects Register/Deregister mutations
	adapters sync.Map     // name (string) → *adapterPos (lock-free reads)
	count    atomic.Int32 // number of registered adapters (for fast empty check)
}

// New returns an empty Tracker with no registered adapters.
func New() *Tracker {
	return &Tracker{}
}

// Register adds an adapter with an initial LSN (typically 0). Safe to call
// concurrently with Ack and MinAckedLSN.
func (t *Tracker) Register(name string, initialLSN uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pos := &adapterPos{name: name}
	pos.lsn.Store(initialLSN)
	if _, loaded := t.adapters.LoadOrStore(name, pos); !loaded {
		t.count.Add(1)
	} else {
		// Overwrite existing entry.
		t.adapters.Store(name, pos)
	}
}

// Ack records that the named adapter has successfully processed up to lsn.
// Only advances forward — a lower lsn is ignored. Lock-free: uses sync.Map
// load (no mutex) and an atomic compare-and-swap loop. Safe to call from any
// goroutine.
func (t *Tracker) Ack(name string, lsn uint64) {
	val, ok := t.adapters.Load(name)
	if !ok {
		return
	}
	pos := val.(*adapterPos)
	for {
		cur := pos.lsn.Load()
		if lsn <= cur {
			return
		}
		if pos.lsn.CompareAndSwap(cur, lsn) {
			metrics.AckPosition.WithLabelValues(name).Set(float64(lsn))
			return
		}
	}
}

// Deregister removes an adapter from the tracker. A dead adapter at LSN=0
// would otherwise block the cooperative checkpoint from ever advancing.
// Safe to call concurrently with Ack and MinAckedLSN.
func (t *Tracker) Deregister(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, loaded := t.adapters.LoadAndDelete(name); loaded {
		t.count.Add(-1)
	}
}

// MinAckedLSN returns the minimum acknowledged LSN across all registered
// adapters. Returns 0 if no adapters are registered or none have acked yet.
// Updates the CooperativeCheckpointLSN metric.
func (t *Tracker) MinAckedLSN() uint64 {
	if t.count.Load() == 0 {
		return 0
	}
	var min uint64
	first := true
	t.adapters.Range(func(_, value any) bool {
		pos := value.(*adapterPos)
		lsn := pos.lsn.Load()
		if first || lsn < min {
			min = lsn
			first = false
		}
		return true
	})
	if first {
		// No adapters found (possible race with Deregister).
		return 0
	}
	metrics.CooperativeCheckpointLSN.Set(float64(min))
	return min
}
