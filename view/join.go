package view

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
)

type joinEntry struct {
	arrivalTime time.Time
	payload     map[string]any
}

type joinBuffer struct {
	entries map[string][]joinEntry // keyed by join key value (as string)
}

func newJoinBuffer() *joinBuffer {
	return &joinBuffer{entries: make(map[string][]joinEntry)}
}

// prune removes entries older than within from the buffer for the given key.
func (b *joinBuffer) prune(key string, now time.Time, within time.Duration) {
	entries := b.entries[key]
	n := 0
	for _, e := range entries {
		if now.Sub(e.arrivalTime) <= within {
			entries[n] = e
			n++
		}
	}
	if n == 0 {
		delete(b.entries, key)
	} else {
		b.entries[key] = entries[:n]
	}
}

// IntervalJoinWindow matches events from two streams whose key fields match
// and whose arrival times are within a configurable tolerance.
type IntervalJoinWindow struct {
	def      *JoinDef
	leftBuf  *joinBuffer
	rightBuf *joinBuffer
	mu       sync.Mutex
}

// NewIntervalJoinWindow creates an interval join window from a JoinDef.
func NewIntervalJoinWindow(def *JoinDef) *IntervalJoinWindow {
	return &IntervalJoinWindow{
		def:      def,
		leftBuf:  newJoinBuffer(),
		rightBuf: newJoinBuffer(),
	}
}

// resolveJoinKey navigates a dotted path in a payload map and returns the value as a string.
func resolveJoinKey(payload map[string]any, field string) string {
	if payload == nil || field == "" {
		return ""
	}
	parts := strings.Split(field, ".")
	// Strip leading "payload" component: the engine passes the raw JSON map as
	// payload, so "payload.row.id" should navigate ["row","id"].
	if len(parts) > 1 && parts[0] == "payload" {
		parts = parts[1:]
	}
	var val any = payload
	for _, part := range parts {
		m, ok := val.(map[string]any)
		if !ok {
			return ""
		}
		val = m[part]
	}
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

// AddLeft processes an event from the left stream.
// Returns any matched pair events found.
func (j *IntervalJoinWindow) AddLeft(arrival time.Time, payload map[string]any) []event.Event {
	j.mu.Lock()
	defer j.mu.Unlock()

	key := resolveJoinKey(payload, j.def.LeftKey)
	if key == "" {
		return nil
	}

	j.leftBuf.prune(key, arrival, j.def.Within)
	j.rightBuf.prune(key, arrival, j.def.Within)

	var results []event.Event
	if rightEntries, ok := j.rightBuf.entries[key]; ok {
		for _, re := range rightEntries {
			diff := arrival.Sub(re.arrivalTime)
			if diff < 0 {
				diff = -diff
			}
			if diff <= j.def.Within {
				if ev := j.emitMatch(payload, re.payload); ev != nil {
					results = append(results, *ev)
				}
			}
		}
	}

	j.leftBuf.entries[key] = append(j.leftBuf.entries[key], joinEntry{arrivalTime: arrival, payload: payload})
	return results
}

// AddRight processes an event from the right stream. Symmetric to AddLeft.
func (j *IntervalJoinWindow) AddRight(arrival time.Time, payload map[string]any) []event.Event {
	j.mu.Lock()
	defer j.mu.Unlock()

	key := resolveJoinKey(payload, j.def.RightKey)
	if key == "" {
		return nil
	}

	j.leftBuf.prune(key, arrival, j.def.Within)
	j.rightBuf.prune(key, arrival, j.def.Within)

	var results []event.Event
	if leftEntries, ok := j.leftBuf.entries[key]; ok {
		for _, le := range leftEntries {
			diff := arrival.Sub(le.arrivalTime)
			if diff < 0 {
				diff = -diff
			}
			if diff <= j.def.Within {
				if ev := j.emitMatch(le.payload, payload); ev != nil {
					results = append(results, *ev)
				}
			}
		}
	}

	j.rightBuf.entries[key] = append(j.rightBuf.entries[key], joinEntry{arrivalTime: arrival, payload: payload})
	return results
}

// emitMatch creates a VIEW_RESULT event from a matched left/right pair.
func (j *IntervalJoinWindow) emitMatch(leftPayload, rightPayload map[string]any) *event.Event {
	var outPayload map[string]any
	if len(j.def.SelectItems) == 0 {
		// Default: include full payloads under alias keys.
		outPayload = map[string]any{
			j.def.LeftAlias:  leftPayload,
			j.def.RightAlias: rightPayload,
		}
	} else {
		outPayload = make(map[string]any)
		for _, si := range j.def.SelectItems {
			var src map[string]any
			if si.SideAlias == j.def.LeftAlias {
				src = leftPayload
			} else {
				src = rightPayload
			}
			outPayload[si.OutputKey] = resolveJoinKey(src, si.Field)
		}
	}

	channel := "pgcdc:_view:" + j.def.Name
	payloadBytes, err := json.Marshal(outPayload)
	if err != nil {
		return nil
	}
	ev, err := event.New(channel, "VIEW_RESULT", payloadBytes, "view")
	if err != nil {
		return nil
	}
	return &ev
}
