package transform

import "github.com/florinutz/pgcdc/event"

// RenameFields returns a transform that renames payload keys according to the
// mapping (old → new). Missing source keys are ignored. Existing target keys
// are overwritten.
func RenameFields(mapping map[string]string) TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		return withPayload(ev, func(m map[string]any) {
			for old, new_ := range mapping {
				val, ok := m[old]
				if !ok {
					continue
				}
				delete(m, old)
				m[new_] = val
			}
		})
	}
}
