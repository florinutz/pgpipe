// Package testutil provides test helpers importable from any package.
package testutil

import (
	"encoding/json"

	"github.com/florinutz/pgcdc/event"
)

// MakeEvent creates a test event with the given channel and JSON payload.
// Uses "INSERT" as the default operation and "test" as the source.
func MakeEvent(channel, payloadJSON string) event.Event {
	ev, err := event.New(channel, "INSERT", json.RawMessage(payloadJSON), "test")
	if err != nil {
		panic("testutil.MakeEvent: " + err.Error())
	}
	return ev
}

// MakeEvents creates n test events on the given channel.
func MakeEvents(n int, channel, payloadJSON string) []event.Event {
	evs := make([]event.Event, n)
	for i := range evs {
		evs[i] = MakeEvent(channel, payloadJSON)
	}
	return evs
}
