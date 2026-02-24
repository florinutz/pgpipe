package encoding

import (
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// Encoder encodes event data into a wire format (Avro, Protobuf, or JSON passthrough).
type Encoder interface {
	// Encode serializes data for the given event. The data parameter is what the
	// adapter would normally send (Kafka: ev.Payload, NATS: json.Marshal(ev)).
	// Returns encoded bytes suitable for the message value.
	Encode(ev event.Event, data []byte) ([]byte, error)

	// ContentType returns the MIME type for encoded data (e.g. "application/json",
	// "application/avro", "application/x-protobuf").
	ContentType() string

	// Close releases any resources held by the encoder.
	Close() error
}

// EncodingType identifies a wire encoding format.
type EncodingType string

const (
	EncodingJSON     EncodingType = "json"
	EncodingAvro     EncodingType = "avro"
	EncodingProtobuf EncodingType = "protobuf"
)

// ParseEncodingType parses an encoding type string. Returns an error for unknown types.
func ParseEncodingType(s string) (EncodingType, error) {
	switch s {
	case "json", "":
		return EncodingJSON, nil
	case "avro":
		return EncodingAvro, nil
	case "protobuf":
		return EncodingProtobuf, nil
	default:
		return "", fmt.Errorf("unknown encoding type: %q (expected json, avro, or protobuf)", s)
	}
}
