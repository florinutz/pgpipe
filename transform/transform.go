package transform

import (
	"encoding/json"
	"errors"

	"github.com/florinutz/pgcdc/event"
)

// TransformFunc transforms an event. Return ErrDropEvent to silently skip the
// event. Any other error causes the event to be skipped with an error metric.
type TransformFunc func(event.Event) (event.Event, error)

// ErrDropEvent is a sentinel error returned by filter transforms to silently
// drop an event from the pipeline.
var ErrDropEvent = errors.New("drop event")

// Chain composes transforms left-to-right. Short-circuits on error or drop.
// Returns nil if no functions are provided.
func Chain(fns ...TransformFunc) TransformFunc {
	if len(fns) == 0 {
		return nil
	}
	if len(fns) == 1 {
		return fns[0]
	}
	return func(ev event.Event) (event.Event, error) {
		var err error
		for _, fn := range fns {
			ev, err = fn(ev)
			if err != nil {
				return ev, err
			}
		}
		return ev, nil
	}
}

// withPayload unmarshals the event payload as a JSON object, calls the mutator,
// and remarshals. Returns the event unchanged if payload is empty or not a JSON
// object (e.g. array, string, number).
func withPayload(ev event.Event, mutate func(map[string]any)) (event.Event, error) {
	if len(ev.Payload) == 0 {
		return ev, nil
	}

	var m map[string]any
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		// Not a JSON object — return unchanged.
		return ev, nil
	}

	mutate(m)

	raw, err := json.Marshal(m)
	if err != nil {
		return ev, err
	}
	ev.Payload = raw
	return ev, nil
}
