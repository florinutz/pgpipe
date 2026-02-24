package registry

import (
	"encoding/binary"
	"fmt"
)

// WireEncode prepends the Confluent wire format header to encoded data:
// [0x00][4-byte big-endian schema ID][payload].
func WireEncode(schemaID int, data []byte) []byte {
	buf := make([]byte, 5+len(data))
	buf[0] = 0x00 // magic byte
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	copy(buf[5:], data)
	return buf
}

// WireDecode strips the Confluent wire format header and returns the schema ID
// and payload. Returns an error if the data is too short or the magic byte is wrong.
func WireDecode(data []byte) (schemaID int, payload []byte, err error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("wire format too short: %d bytes", len(data))
	}
	if data[0] != 0x00 {
		return 0, nil, fmt.Errorf("invalid wire format magic byte: 0x%02x", data[0])
	}
	schemaID = int(binary.BigEndian.Uint32(data[1:5]))
	return schemaID, data[5:], nil
}

// WireEncodeProtobuf prepends the Confluent wire format header for Protobuf:
// [0x00][4-byte big-endian schema ID][varint message index count][varint message indexes...][payload].
// For a single top-level message, the index array is [0] encoded as two varints: count=1, index=0.
func WireEncodeProtobuf(schemaID int, data []byte) []byte {
	// For a single top-level message: [count=0 as varint] means "use index 0".
	// Confluent convention: a single 0x00 byte means message index [0].
	buf := make([]byte, 6+len(data))
	buf[0] = 0x00 // magic byte
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	buf[5] = 0x00 // varint 0 = array length 0 = first message in .proto
	copy(buf[6:], data)
	return buf
}
