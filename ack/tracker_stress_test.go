package ack_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/florinutz/pgcdc/ack"
)

func TestTracker_StressManyAdapters(t *testing.T) {
	tr := ack.New()
	const adapterCount = 100
	const acksPerAdapter = 1000

	for i := 0; i < adapterCount; i++ {
		tr.Register(fmt.Sprintf("a-%d", i), 0)
	}

	var wg sync.WaitGroup
	for i := 0; i < adapterCount; i++ {
		wg.Add(1)
		name := fmt.Sprintf("a-%d", i)
		go func() {
			defer wg.Done()
			for j := 1; j <= acksPerAdapter; j++ {
				tr.Ack(name, uint64(j))
			}
		}()
	}
	wg.Wait()

	got := tr.MinAckedLSN()
	if got != acksPerAdapter {
		t.Errorf("MinAckedLSN = %d, want %d", got, acksPerAdapter)
	}
}
