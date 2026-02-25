package kafkaserver

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// readRequest reads a length-prefixed Kafka request from the wire.
// Returns the header and the remaining body bytes.
func readRequest(r io.Reader) (requestHeader, []byte, error) {
	// 4-byte length prefix.
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return requestHeader{}, nil, fmt.Errorf("read length: %w", err)
	}
	if length <= 0 || length > 100*1024*1024 { // 100MB sanity limit
		return requestHeader{}, nil, fmt.Errorf("invalid request length: %d", length)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return requestHeader{}, nil, fmt.Errorf("read body: %w", err)
	}

	if len(buf) < 8 {
		return requestHeader{}, nil, fmt.Errorf("request too short: %d bytes", len(buf))
	}

	var hdr requestHeader
	hdr.APIKey = int16(binary.BigEndian.Uint16(buf[0:2]))
	hdr.APIVersion = int16(binary.BigEndian.Uint16(buf[2:4]))
	hdr.CorrelationID = int32(binary.BigEndian.Uint32(buf[4:8]))

	off := 8

	// ApiVersions v3+ uses flexible header (request header v2):
	// compact_nullable_string for client_id + tagged_fields.
	if hdr.APIKey == apiApiVersions && hdr.APIVersion >= 3 {
		if off < len(buf) {
			// Compact nullable string: unsigned varint length.
			// 0 = null, N = N-1 bytes of data.
			n, bytesRead := readUvarintBuf(buf[off:])
			off += bytesRead
			if n > 1 {
				clientIDLen := int(n) - 1
				if off+clientIDLen <= len(buf) {
					hdr.ClientID = string(buf[off : off+clientIDLen])
					off += clientIDLen
				}
			}
			// Skip tagged fields (unsigned varint count, then tag+size+data each).
			if off < len(buf) {
				tagCount, bytesRead := readUvarintBuf(buf[off:])
				off += bytesRead
				for i := uint64(0); i < tagCount && off < len(buf); i++ {
					_, br := readUvarintBuf(buf[off:]) // tag key
					off += br
					sz, br := readUvarintBuf(buf[off:]) // data size
					off += br
					off += int(sz) // data
				}
			}
		}
	} else {
		// Standard nullable string (int16 length, -1 = null).
		if off+2 > len(buf) {
			return hdr, nil, nil
		}
		clientIDLen := int16(binary.BigEndian.Uint16(buf[off : off+2]))
		off += 2
		if clientIDLen > 0 {
			if off+int(clientIDLen) > len(buf) {
				return requestHeader{}, nil, fmt.Errorf("client id truncated")
			}
			hdr.ClientID = string(buf[off : off+int(clientIDLen)])
			off += int(clientIDLen)
		}
	}

	if off > len(buf) {
		off = len(buf)
	}
	return hdr, buf[off:], nil
}

// readUvarintBuf reads an unsigned varint from buf and returns the value
// and the number of bytes consumed. Returns (0, 1) on error for robustness.
func readUvarintBuf(buf []byte) (uint64, int) {
	var val uint64
	var shift uint
	for i := 0; i < len(buf); i++ {
		b := buf[i]
		val |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return val, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, i + 1
		}
	}
	return 0, len(buf)
}

// writeResponse writes a length-prefixed Kafka response to the writer.
func writeResponse(w io.Writer, correlationID int32, body []byte) error {
	// Total frame: 4 bytes correlation ID + body.
	length := int32(4 + len(body))

	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, correlationID); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// --- Binary encoding helpers ---

type encoder struct {
	buf []byte
}

func newEncoder(capacity int) *encoder {
	return &encoder{buf: make([]byte, 0, capacity)}
}

func (e *encoder) putInt8(v int8)   { e.buf = append(e.buf, byte(v)) }
func (e *encoder) putInt16(v int16) { e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(v)) }
func (e *encoder) putInt32(v int32) { e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(v)) }
func (e *encoder) putInt64(v int64) { e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(v)) }
func (e *encoder) putString(s string) {
	e.putInt16(int16(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *encoder) putNullableString(s string) {
	if s == "" {
		e.putInt16(-1)
		return
	}
	e.putString(s)
}

func (e *encoder) putBytes(b []byte) {
	if b == nil {
		e.putInt32(-1)
		return
	}
	e.putInt32(int32(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *encoder) putArrayLen(n int) {
	e.putInt32(int32(n))
}

// putUvarint encodes an unsigned varint (used in compact/flexible encoding).
func (e *encoder) putUvarint(v uint64) {
	for v >= 0x80 {
		e.buf = append(e.buf, byte(v)|0x80)
		v >>= 7
	}
	e.buf = append(e.buf, byte(v))
}

// putCompactArrayLen encodes a compact array length (flexible encoding).
// compact length = actual_length + 1 (0 = null, 1 = empty, N+1 = N elements).
func (e *encoder) putCompactArrayLen(n int) {
	e.putUvarint(uint64(n + 1))
}

// putTagBuffer encodes an empty tagged fields buffer (unsigned varint 0).
func (e *encoder) putTagBuffer() {
	e.buf = append(e.buf, 0)
}

func (e *encoder) bytes() []byte {
	return e.buf
}

// --- Binary decoding helpers ---

type decoder struct {
	buf []byte
	off int
	err error
}

func newDecoder(buf []byte) *decoder {
	return &decoder{buf: buf}
}

func (d *decoder) readInt8() int8 {
	if d.err != nil {
		return 0
	}
	if d.off+1 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := int8(d.buf[d.off])
	d.off++
	return v
}

func (d *decoder) readInt16() int16 {
	if d.err != nil {
		return 0
	}
	if d.off+2 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := int16(binary.BigEndian.Uint16(d.buf[d.off : d.off+2]))
	d.off += 2
	return v
}

func (d *decoder) readInt32() int32 {
	if d.err != nil {
		return 0
	}
	if d.off+4 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := int32(binary.BigEndian.Uint32(d.buf[d.off : d.off+4]))
	d.off += 4
	return v
}

func (d *decoder) readInt64() int64 {
	if d.err != nil {
		return 0
	}
	if d.off+8 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := int64(binary.BigEndian.Uint64(d.buf[d.off : d.off+8]))
	d.off += 8
	return v
}

func (d *decoder) readString() string {
	length := d.readInt16()
	if d.err != nil {
		return ""
	}
	if length < 0 {
		return ""
	}
	if d.off+int(length) > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return ""
	}
	s := string(d.buf[d.off : d.off+int(length)])
	d.off += int(length)
	return s
}

func (d *decoder) readBytes() []byte {
	length := d.readInt32()
	if d.err != nil {
		return nil
	}
	if length < 0 {
		return nil
	}
	if d.off+int(length) > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	b := make([]byte, length)
	copy(b, d.buf[d.off:d.off+int(length)])
	d.off += int(length)
	return b
}

func (d *decoder) readArrayLen() int32 {
	return d.readInt32()
}

// --- Varint encoding/decoding (signed zigzag, used in RecordBatch v2) ---

func putVarint(buf []byte, v int64) int {
	uv := uint64((v << 1) ^ (v >> 63)) // zigzag
	i := 0
	for uv >= 0x80 {
		buf[i] = byte(uv) | 0x80
		uv >>= 7
		i++
	}
	buf[i] = byte(uv)
	return i + 1
}

func appendVarint(dst []byte, v int64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := putVarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func readVarint(buf []byte, off int) (int64, int, error) {
	var uv uint64
	var shift uint
	i := off
	for {
		if i >= len(buf) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		b := buf[i]
		i++
		uv |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, 0, fmt.Errorf("varint overflow")
		}
	}
	// zigzag decode
	v := int64((uv >> 1) ^ -(uv & 1))
	return v, i - off, nil
}

// varintLen returns the encoded length of a zigzag varint.
func varintLen(v int64) int {
	uv := uint64((v << 1) ^ (v >> 63))
	n := 1
	for uv >= 0x80 {
		uv >>= 7
		n++
	}
	return n
}

// --- RecordBatch v2 encoding ---

// encodeRecordBatch encodes records into a Kafka RecordBatch v2 format.
// This is the format returned in Fetch responses.
func encodeRecordBatch(records []record) []byte {
	if len(records) == 0 {
		return nil
	}

	baseOffset := records[0].Offset
	baseTimestamp := records[0].Timestamp

	// Encode individual records first.
	var recordsData []byte
	for i, rec := range records {
		recordsData = encodeRecord(recordsData, rec, baseOffset, baseTimestamp, i)
	}

	lastOffset := records[len(records)-1].Offset
	maxTimestamp := baseTimestamp
	for _, rec := range records {
		if rec.Timestamp > maxTimestamp {
			maxTimestamp = rec.Timestamp
		}
	}

	// Build the batch (after the baseOffset + batchLength fields).
	// Layout:
	//   partitionLeaderEpoch int32
	//   magic int8 (=2)
	//   crc uint32
	//   attributes int16
	//   lastOffsetDelta int32
	//   baseTimestamp int64
	//   maxTimestamp int64
	//   producerID int64
	//   producerEpoch int16
	//   baseSequence int32
	//   records array length int32
	//   records bytes

	// Inner section (after CRC).
	inner := newEncoder(64 + len(recordsData))
	inner.putInt16(0)                              // attributes
	inner.putInt32(int32(lastOffset - baseOffset)) // lastOffsetDelta
	inner.putInt64(baseTimestamp)                  // baseTimestamp
	inner.putInt64(maxTimestamp)                   // maxTimestamp
	inner.putInt64(-1)                             // producerID
	inner.putInt16(-1)                             // producerEpoch
	inner.putInt32(-1)                             // baseSequence
	inner.putInt32(int32(len(records)))            // record count
	inner.buf = append(inner.buf, recordsData...)

	crc := crc32.Checksum(inner.bytes(), crc32cTable)

	// Full batch.
	batch := newEncoder(12 + 4 + 1 + 4 + len(inner.bytes()))
	batch.putInt64(baseOffset)                           // baseOffset
	batchLength := int32(4 + 1 + 4 + len(inner.bytes())) // partitionLeaderEpoch + magic + crc + inner
	batch.putInt32(batchLength)                          // batchLength
	batch.putInt32(0)                                    // partitionLeaderEpoch
	batch.putInt8(2)                                     // magic
	batch.putInt32(int32(crc))                           // crc
	batch.buf = append(batch.buf, inner.bytes()...)

	return batch.bytes()
}

// encodeRecord encodes a single record within a RecordBatch v2.
func encodeRecord(dst []byte, rec record, baseOffset, baseTimestamp int64, index int) []byte {
	// Record body (before length prefix).
	var body []byte
	body = appendVarint(body, 0)                           // attributes
	body = appendVarint(body, rec.Timestamp-baseTimestamp) // timestampDelta
	body = appendVarint(body, rec.Offset-baseOffset)       // offsetDelta

	// Key.
	if rec.Key == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(rec.Key)))
		body = append(body, rec.Key...)
	}

	// Value.
	if rec.Value == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(rec.Value)))
		body = append(body, rec.Value...)
	}

	// Headers.
	body = appendVarint(body, int64(len(rec.Headers)))
	for _, h := range rec.Headers {
		body = appendVarint(body, int64(len(h.Key)))
		body = append(body, h.Key...)
		if h.Value == nil {
			body = appendVarint(body, -1)
		} else {
			body = appendVarint(body, int64(len(h.Value)))
			body = append(body, h.Value...)
		}
	}

	// Length prefix (varint).
	dst = appendVarint(dst, int64(len(body)))
	dst = append(dst, body...)

	_ = index // reserved for future use
	return dst
}
