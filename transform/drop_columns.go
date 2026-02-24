package transform

import "github.com/florinutz/pgcdc/event"

// DropColumns returns a transform that deletes the named keys from the event
// payload. Missing keys are silently ignored.
func DropColumns(columns ...string) TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		return withPayload(ev, func(m map[string]any) {
			for _, col := range columns {
				delete(m, col)
			}
		})
	}
}
