package kafkaserver

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func newTestHandler() *handler {
	brk := newBroker(4, 100, "id")
	return &handler{
		broker:  brk,
		groups:  newGroupCoordinator(30*time.Second, slog.Default()),
		offsets: nil,
		nodeID:  0,
		host:    "localhost",
		port:    9092,
		logger:  slog.Default(),
	}
}

func TestHandleJoinGroup_DecodeError(t *testing.T) {
	h := newTestHandler()
	resp := h.handleJoinGroup(requestHeader{APIKey: apiJoinGroup}, nil)
	d := newDecoder(resp)
	errCode := d.readInt16()
	if errCode != errInvalidRequest {
		t.Fatalf("expected error code %d (errInvalidRequest), got %d", errInvalidRequest, errCode)
	}
}

func TestHandleSyncGroup_DecodeError(t *testing.T) {
	h := newTestHandler()
	resp := h.handleSyncGroup(requestHeader{APIKey: apiSyncGroup}, nil)
	d := newDecoder(resp)
	errCode := d.readInt16()
	if errCode != errInvalidRequest {
		t.Fatalf("expected error code %d (errInvalidRequest), got %d", errInvalidRequest, errCode)
	}
}

func TestHandleHeartbeat_DecodeError(t *testing.T) {
	h := newTestHandler()
	resp := h.handleHeartbeat(requestHeader{APIKey: apiHeartbeat}, nil)
	d := newDecoder(resp)
	errCode := d.readInt16()
	if errCode != errInvalidRequest {
		t.Fatalf("expected error code %d (errInvalidRequest), got %d", errInvalidRequest, errCode)
	}
}

func TestHandleLeaveGroup_DecodeError(t *testing.T) {
	h := newTestHandler()
	resp := h.handleLeaveGroup(requestHeader{APIKey: apiLeaveGroup}, nil)
	d := newDecoder(resp)
	errCode := d.readInt16()
	if errCode != errInvalidRequest {
		t.Fatalf("expected error code %d (errInvalidRequest), got %d", errInvalidRequest, errCode)
	}
}

func TestHandleApiVersions_V0(t *testing.T) {
	h := newTestHandler()
	resp := h.handleApiVersions(requestHeader{APIKey: apiApiVersions, APIVersion: 0})
	d := newDecoder(resp)

	errCode := d.readInt16()
	if errCode != errNone {
		t.Fatalf("error code: got %d, want %d", errCode, errNone)
	}

	count := d.readArrayLen()
	if count < 1 {
		t.Fatalf("api versions count: got %d, want >= 1", count)
	}

	found := false
	for i := int32(0); i < count; i++ {
		key := d.readInt16()
		_ = d.readInt16() // min
		_ = d.readInt16() // max
		if key == apiApiVersions {
			found = true
		}
	}
	if !found {
		t.Error("ApiVersions key not found in supported APIs")
	}

	// V0 should NOT have throttle_time_ms — we should be at end of buffer.
	if d.err != nil {
		t.Fatalf("decode error: %v", d.err)
	}
}

func TestHandleApiVersions_V1(t *testing.T) {
	h := newTestHandler()
	resp := h.handleApiVersions(requestHeader{APIKey: apiApiVersions, APIVersion: 1})
	d := newDecoder(resp)

	errCode := d.readInt16()
	if errCode != errNone {
		t.Fatalf("error code: got %d, want %d", errCode, errNone)
	}

	count := d.readArrayLen()
	for i := int32(0); i < count; i++ {
		_ = d.readInt16() // key
		_ = d.readInt16() // min
		_ = d.readInt16() // max
	}

	// V1+ has throttle_time_ms.
	throttle := d.readInt32()
	if throttle != 0 {
		t.Errorf("throttle_time_ms: got %d, want 0", throttle)
	}
	if d.err != nil {
		t.Fatalf("decode error: %v", d.err)
	}
}

func TestHandleApiVersions_V3_Flexible(t *testing.T) {
	h := newTestHandler()
	resp := h.handleApiVersions(requestHeader{APIKey: apiApiVersions, APIVersion: 3})

	// V3 uses compact arrays. Manual decode:
	off := 0

	// error_code (int16)
	if off+2 > len(resp) {
		t.Fatal("response too short for error_code")
	}
	errCode := int16(resp[off])<<8 | int16(resp[off+1])
	off += 2
	if errCode != errNone {
		t.Fatalf("error code: got %d, want %d", errCode, errNone)
	}

	// compact array length (uvarint)
	compactLen, br := readUvarintBuf(resp[off:])
	off += br
	arrayLen := int(compactLen) - 1 // compact: N+1
	if arrayLen < 1 {
		t.Fatalf("api versions count: got %d, want >= 1", arrayLen)
	}

	for i := 0; i < arrayLen; i++ {
		off += 2 // api key
		off += 2 // min version
		off += 2 // max version
		// per-entry tagged fields (uvarint 0)
		_, br := readUvarintBuf(resp[off:])
		off += br
	}

	// throttle_time_ms (int32)
	off += 4

	// top-level tagged fields
	_, br = readUvarintBuf(resp[off:])
	off += br

	if off != len(resp) {
		t.Errorf("expected to consume entire response (%d bytes), consumed %d", len(resp), off)
	}
}

func TestHandleMetadata_NoTopics(t *testing.T) {
	h := newTestHandler()

	// Pre-create some topics.
	h.broker.getOrCreateTopic("pgcdc.orders")
	h.broker.getOrCreateTopic("pgcdc.users")

	// Empty topic request (array len 0).
	enc := newEncoder(4)
	enc.putArrayLen(0)
	resp := h.handleMetadata(requestHeader{APIKey: apiMetadata}, enc.bytes())

	d := newDecoder(resp)

	// Brokers array.
	brokerCount := d.readArrayLen()
	if brokerCount != 1 {
		t.Fatalf("broker count: got %d, want 1", brokerCount)
	}
	_ = d.readInt32()  // node_id
	_ = d.readString() // host
	_ = d.readInt32()  // port

	// Topics array — should contain all known topics.
	topicCount := d.readArrayLen()
	if topicCount < 2 {
		t.Fatalf("topic count: got %d, want >= 2", topicCount)
	}
}

func TestHandleMetadata_SpecificTopics(t *testing.T) {
	h := newTestHandler()

	h.broker.getOrCreateTopic("pgcdc.orders")
	h.broker.getOrCreateTopic("pgcdc.users")

	// Request only "pgcdc.orders".
	enc := newEncoder(32)
	enc.putArrayLen(1)
	enc.putString("pgcdc.orders")
	resp := h.handleMetadata(requestHeader{APIKey: apiMetadata}, enc.bytes())

	d := newDecoder(resp)

	// Skip brokers.
	brokerCount := d.readArrayLen()
	for i := int32(0); i < brokerCount; i++ {
		_ = d.readInt32()
		_ = d.readString()
		_ = d.readInt32()
	}

	topicCount := d.readArrayLen()
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}

	errCode := d.readInt16()
	if errCode != errNone {
		t.Fatalf("topic error code: got %d", errCode)
	}
	topicName := d.readString()
	if topicName != "pgcdc.orders" {
		t.Fatalf("topic name: got %q, want %q", topicName, "pgcdc.orders")
	}
}

func TestHandleFindCoordinator(t *testing.T) {
	h := newTestHandler()
	resp := h.handleFindCoordinator(requestHeader{APIKey: apiFindCoordinator}, nil)

	d := newDecoder(resp)
	errCode := d.readInt16()
	if errCode != errNone {
		t.Fatalf("error code: got %d, want %d", errCode, errNone)
	}
	nodeID := d.readInt32()
	if nodeID != 0 {
		t.Errorf("nodeID: got %d, want 0", nodeID)
	}
	host := d.readString()
	if host != "localhost" {
		t.Errorf("host: got %q, want %q", host, "localhost")
	}
	port := d.readInt32()
	if port != 9092 {
		t.Errorf("port: got %d, want 9092", port)
	}
}

func TestHandleListOffsets_EarliestLatest(t *testing.T) {
	h := newTestHandler()

	// Create topic and add some records.
	topic := h.broker.getOrCreateTopic("test.topic")
	for i := 0; i < 10; i++ {
		topic.partitions[0].append(record{Key: []byte("k"), Value: []byte(`{}`)})
	}

	// Request earliest (-2) and latest (-1) for partition 0.
	enc := newEncoder(64)
	enc.putInt32(-1)   // replica_id
	enc.putArrayLen(1) // 1 topic
	enc.putString("test.topic")
	enc.putArrayLen(2) // 2 partitions
	enc.putInt32(0)    // partition 0
	enc.putInt64(-2)   // earliest
	enc.putInt32(0)    // partition 0 again
	enc.putInt64(-1)   // latest

	resp := h.handleListOffsets(requestHeader{APIKey: apiListOffsets, APIVersion: 1}, enc.bytes())
	d := newDecoder(resp)

	topicCount := d.readArrayLen()
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}
	_ = d.readString() // topic name

	partCount := d.readArrayLen()
	if partCount != 2 {
		t.Fatalf("partition count: got %d, want 2", partCount)
	}

	// Earliest.
	_ = d.readInt32() // partition
	ec1 := d.readInt16()
	if ec1 != errNone {
		t.Fatalf("earliest error: %d", ec1)
	}
	_ = d.readInt64() // timestamp
	earliest := d.readInt64()
	if earliest != 0 {
		t.Errorf("earliest offset: got %d, want 0", earliest)
	}

	// Latest.
	_ = d.readInt32() // partition
	ec2 := d.readInt16()
	if ec2 != errNone {
		t.Fatalf("latest error: %d", ec2)
	}
	_ = d.readInt64() // timestamp
	latest := d.readInt64()
	if latest != 10 {
		t.Errorf("latest offset: got %d, want 10", latest)
	}
}

func TestBuildFetchResponse_NoData(t *testing.T) {
	h := newTestHandler()
	h.broker.getOrCreateTopic("empty.topic")

	req := fetchRequest{
		Topics: []fetchRequestTopic{
			{
				Topic: "empty.topic",
				Partitions: []fetchRequestPartition{
					{Partition: 0, FetchOffset: 0, MaxBytes: 1024 * 1024},
				},
			},
		},
	}

	resp, hasData := h.buildFetchResponse(req)
	if hasData {
		t.Error("expected no data")
	}
	if len(resp) == 0 {
		t.Error("response should not be empty")
	}
}

func TestDispatch_UnsupportedAPIKey(t *testing.T) {
	h := newTestHandler()
	resp := h.dispatch(context.Background(), requestHeader{APIKey: 99}, nil)

	d := newDecoder(resp)
	errCode := d.readInt16()
	if errCode != errUnsupportedVersion {
		t.Fatalf("expected error code %d (errUnsupportedVersion), got %d", errUnsupportedVersion, errCode)
	}
}

func TestFindLSNForOffset_OutOfRange(t *testing.T) {
	h := newTestHandler()
	h.broker.getOrCreateTopic("test.topic")

	lsn := h.findLSNForOffset("test.topic", 0, 999)
	if lsn != 0 {
		t.Errorf("expected LSN 0 for out-of-range offset, got %d", lsn)
	}
}

func TestFindLSNForOffset_ValidOffset(t *testing.T) {
	h := newTestHandler()
	topic := h.broker.getOrCreateTopic("test.topic")

	topic.partitions[0].append(record{
		Key:   []byte("k"),
		Value: []byte(`{}`),
		LSN:   12345,
	})

	lsn := h.findLSNForOffset("test.topic", 0, 0)
	if lsn != 12345 {
		t.Errorf("expected LSN 12345, got %d", lsn)
	}
}

func TestFindLSNForOffset_NilTopic(t *testing.T) {
	h := newTestHandler()
	lsn := h.findLSNForOffset("nonexistent", 0, 0)
	if lsn != 0 {
		t.Errorf("expected LSN 0 for nil topic, got %d", lsn)
	}
}

func TestHandleOffsetCommit_NoStore(t *testing.T) {
	h := newTestHandler()

	enc := newEncoder(64)
	enc.putString("test-group") // group_id
	enc.putInt32(1)             // generation_id
	enc.putString("member-1")   // member_id
	enc.putInt64(0)             // retention_time_ms
	enc.putArrayLen(1)          // 1 topic
	enc.putString("test.topic")
	enc.putArrayLen(1) // 1 partition
	enc.putInt32(0)    // partition 0
	enc.putInt64(42)   // offset 42
	enc.putString("")  // metadata

	resp := h.handleOffsetCommit(context.Background(), requestHeader{APIKey: apiOffsetCommit, APIVersion: 2}, enc.bytes())
	d := newDecoder(resp)

	topicCount := d.readArrayLen()
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}
	_ = d.readString() // topic name
	partCount := d.readArrayLen()
	if partCount != 1 {
		t.Fatalf("partition count: got %d, want 1", partCount)
	}
	_ = d.readInt32() // partition
	errCode := d.readInt16()
	if errCode != errNone {
		t.Fatalf("error code: got %d, want %d (no store = no error)", errCode, errNone)
	}
}

func TestHandleOffsetFetch_NoStore(t *testing.T) {
	h := newTestHandler()

	enc := newEncoder(32)
	enc.putString("test-group") // group_id
	enc.putArrayLen(1)          // 1 topic
	enc.putString("test.topic")
	enc.putArrayLen(1) // 1 partition
	enc.putInt32(0)    // partition 0

	resp := h.handleOffsetFetch(context.Background(), requestHeader{APIKey: apiOffsetFetch}, enc.bytes())
	d := newDecoder(resp)

	topicCount := d.readArrayLen()
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}
	_ = d.readString() // topic name
	partCount := d.readArrayLen()
	if partCount != 1 {
		t.Fatalf("partition count: got %d, want 1", partCount)
	}
	_ = d.readInt32() // partition
	offset := d.readInt64()
	if offset != -1 {
		t.Errorf("offset: got %d, want -1 (no store)", offset)
	}
}

func TestBuildFetchResponse_WithData(t *testing.T) {
	h := newTestHandler()
	topic := h.broker.getOrCreateTopic("test.topic")

	for i := 0; i < 5; i++ {
		topic.partitions[0].append(record{
			Key:       []byte("k"),
			Value:     []byte(`{"i":1}`),
			Timestamp: int64(i * 1000),
			LSN:       uint64(i + 1),
		})
	}

	req := fetchRequest{
		Topics: []fetchRequestTopic{
			{
				Topic: "test.topic",
				Partitions: []fetchRequestPartition{
					{Partition: 0, FetchOffset: 0, MaxBytes: 1024 * 1024},
				},
			},
		},
	}

	_, hasData := h.buildFetchResponse(req)
	if !hasData {
		t.Error("expected data in response")
	}
}

func TestBuildFetchResponse_UnknownPartition(t *testing.T) {
	h := newTestHandler()
	h.broker.getOrCreateTopic("test.topic")

	req := fetchRequest{
		Topics: []fetchRequestTopic{
			{
				Topic: "test.topic",
				Partitions: []fetchRequestPartition{
					{Partition: 99, FetchOffset: 0, MaxBytes: 1024 * 1024},
				},
			},
		},
	}

	resp, hasData := h.buildFetchResponse(req)
	if hasData {
		t.Error("expected no data for unknown partition")
	}

	// Decode response to verify error code.
	d := newDecoder(resp)
	topicCount := d.readArrayLen()
	if topicCount != 1 {
		t.Fatalf("topic count: %d", topicCount)
	}
	_ = d.readString() // topic
	partCount := d.readArrayLen()
	if partCount != 1 {
		t.Fatalf("part count: %d", partCount)
	}
	_ = d.readInt32() // partition
	ec := d.readInt16()
	if ec != errUnknownTopicOrPartition {
		t.Errorf("error code: got %d, want %d", ec, errUnknownTopicOrPartition)
	}
}
