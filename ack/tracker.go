// Package ack provides cooperative checkpointing for WAL adapters.
// Adapters report the highest LSN they have successfully processed; the
// Tracker exposes MinAckedLSN() so the WAL detector can advance the
// standby status only as far as the slowest adapter has confirmed.
package ack

import (
	"sync"

	"github.com/florinutz/pgcdc/metrics"
)

// Tracker tracks the highest acknowledged LSN per adapter and computes the
// minimum across all registered adapters for cooperative checkpointing.
type Tracker struct {
	mu       sync.Mutex
	adapters map[string]uint64 // name → highest acked LSN
}

// New returns an empty Tracker with no registered adapters.
func New() *Tracker {
	return &Tracker{
		adapters: make(map[string]uint64),
	}
}

// Register adds an adapter with an initial LSN (typically 0). Safe to call
// concurrently with Ack and MinAckedLSN.
func (t *Tracker) Register(name string, initialLSN uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.adapters[name] = initialLSN
}

// Ack records that the named adapter has successfully processed up to lsn.
// Only advances forward — a lower lsn is ignored. Safe to call from any
// goroutine (including adapter goroutines).
func (t *Tracker) Ack(name string, lsn uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if lsn > t.adapters[name] {
		t.adapters[name] = lsn
		metrics.AckPosition.WithLabelValues(name).Set(float64(lsn))
	}
}

// MinAckedLSN returns the minimum acknowledged LSN across all registered
// adapters. Returns 0 if no adapters are registered or none have acked yet.
// Updates the CooperativeCheckpointLSN metric.
func (t *Tracker) MinAckedLSN() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.adapters) == 0 {
		return 0
	}
	var min uint64
	first := true
	for _, lsn := range t.adapters {
		if first || lsn < min {
			min = lsn
			first = false
		}
	}
	metrics.CooperativeCheckpointLSN.Set(float64(min))
	return min
}
