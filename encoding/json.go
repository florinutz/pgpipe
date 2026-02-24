package encoding

import "github.com/florinutz/pgcdc/event"

// JSONEncoder is a passthrough encoder that returns data unchanged.
// It is the default when no encoding is configured.
type JSONEncoder struct{}

func (JSONEncoder) Encode(_ event.Event, data []byte) ([]byte, error) {
	return data, nil
}

func (JSONEncoder) ContentType() string { return "application/json" }
func (JSONEncoder) Close() error        { return nil }
