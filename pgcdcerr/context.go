package pgcdcerr

import (
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// WrapEvent wraps err with adapter name and event context fields (ID, channel, operation).
// Use this for per-event delivery failures to make errors self-describing for log correlation.
func WrapEvent(err error, adapter string, ev event.Event) error {
	return fmt.Errorf("%s[event=%s channel=%s op=%s]: %w",
		adapter, ev.ID, ev.Channel, ev.Operation, err)
}
