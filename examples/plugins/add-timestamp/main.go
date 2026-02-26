package main

import (
	"encoding/json"
	"time"

	"github.com/extism/go-pdk"
)

// Event mirrors the pgcdc event structure for JSON plugins.
type Event struct {
	ID        string          `json:"id"`
	Channel   string          `json:"channel"`
	Operation string          `json:"operation"`
	Payload   json.RawMessage `json:"payload"`
	Source    string          `json:"source"`
	CreatedAt string          `json:"created_at"`
}

// Payload is the inner structure of a pgcdc event payload.
type Payload struct {
	Op    string                 `json:"op"`
	Table string                 `json:"table"`
	Row   map[string]interface{} `json:"row"`
	Old   map[string]interface{} `json:"old,omitempty"`
}

//export transform
func transform() int32 {
	input := pdk.Input()

	var ev Event
	if err := json.Unmarshal(input, &ev); err != nil {
		pdk.SetError(err)
		return 1
	}

	var p Payload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		pdk.SetError(err)
		return 1
	}

	if p.Row != nil {
		p.Row["processed_at"] = time.Now().UTC().Format(time.RFC3339)
	}

	newPayload, err := json.Marshal(p)
	if err != nil {
		pdk.SetError(err)
		return 1
	}
	ev.Payload = newPayload

	output, err := json.Marshal(ev)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	pdk.Output(output)
	return 0
}

func main() {}
