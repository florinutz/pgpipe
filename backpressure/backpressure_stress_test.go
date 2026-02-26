package backpressure

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackpressure_ZoneFlapping(t *testing.T) {
	var lag atomic.Int64
	c := New(500, 2000, 100*time.Millisecond, 0, nil, nil)
	c.SetLagFunc(lag.Load)
	c.SetAdapterPriority("be", PriorityBestEffort)
	c.SetAdapterPriority("norm", PriorityNormal)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Oscillate lag rapidly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			if i%2 == 0 {
				lag.Store(3000) // red
			} else {
				lag.Store(100) // green
			}
			c.evaluate()
			time.Sleep(time.Millisecond)
		}
	}()

	// Multiple goroutines calling WaitResume/IsShed.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if c.IsPaused() {
					waitCtx, waitCancel := context.WithTimeout(ctx, 50*time.Millisecond)
					_ = c.WaitResume(waitCtx)
					waitCancel()
				}
				_ = c.IsShed("be")
				_ = c.IsShed("norm")
				_ = c.Zone()
				_ = c.ThrottleDuration()
			}
		}()
	}

	wg.Wait()
	// If we get here without panic/deadlock, the test passes.
}
