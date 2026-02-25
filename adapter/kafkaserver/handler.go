package kafkaserver

import (
	"context"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/metrics"
)

// handler dispatches Kafka protocol requests to the appropriate handler.
type handler struct {
	broker  *broker
	groups  *groupCoordinator
	offsets *offsetStore
	nodeID  int32
	host    string
	port    int32
	ackFn   adapter.AckFunc
	logger  *slog.Logger
}

// dispatch routes a request to the correct handler based on API key.
func (h *handler) dispatch(ctx context.Context, hdr requestHeader, body []byte) []byte {
	switch hdr.APIKey {
	case apiApiVersions:
		return h.handleApiVersions(hdr)
	case apiMetadata:
		return h.handleMetadata(hdr, body)
	case apiFindCoordinator:
		return h.handleFindCoordinator(hdr, body)
	case apiJoinGroup:
		return h.handleJoinGroup(hdr, body)
	case apiSyncGroup:
		return h.handleSyncGroup(hdr, body)
	case apiHeartbeat:
		return h.handleHeartbeat(hdr, body)
	case apiLeaveGroup:
		return h.handleLeaveGroup(hdr, body)
	case apiListOffsets:
		return h.handleListOffsets(hdr, body)
	case apiFetch:
		return h.handleFetch(ctx, hdr, body)
	case apiOffsetCommit:
		return h.handleOffsetCommit(ctx, hdr, body)
	case apiOffsetFetch:
		return h.handleOffsetFetch(ctx, hdr, body)
	default:
		metrics.KafkaServerProtocolErrors.Inc()
		h.logger.Warn("unsupported api key", "api_key", hdr.APIKey)
		return h.encodeUnsupportedVersion()
	}
}

// --- ApiVersions (key 18) ---

func (h *handler) handleApiVersions(hdr requestHeader) []byte {
	supported := []apiVersionEntry{
		{apiApiVersions, 0, 3},
		{apiMetadata, 0, 0},
		{apiFindCoordinator, 0, 0},
		{apiJoinGroup, 0, 0},
		{apiSyncGroup, 0, 0},
		{apiHeartbeat, 0, 0},
		{apiLeaveGroup, 0, 0},
		{apiListOffsets, 1, 1},
		{apiFetch, 0, 0},
		{apiOffsetCommit, 2, 2},
		{apiOffsetFetch, 0, 0},
	}

	if hdr.APIVersion >= 3 {
		// Flexible encoding (v3+): compact arrays + tagged fields.
		enc := newEncoder(128)
		enc.putInt16(errNone)                  // error_code
		enc.putCompactArrayLen(len(supported)) // compact array length
		for _, entry := range supported {
			enc.putInt16(entry.APIKey)
			enc.putInt16(entry.MinVersion)
			enc.putInt16(entry.MaxVersion)
			enc.putTagBuffer() // per-entry tagged fields
		}
		enc.putInt32(0)    // throttle_time_ms
		enc.putTagBuffer() // top-level tagged fields
		return enc.bytes()
	}

	if hdr.APIVersion >= 1 {
		// v1-v2: non-flexible, with throttle_time_ms at end.
		enc := newEncoder(96)
		enc.putInt16(errNone)
		enc.putArrayLen(len(supported))
		for _, entry := range supported {
			enc.putInt16(entry.APIKey)
			enc.putInt16(entry.MinVersion)
			enc.putInt16(entry.MaxVersion)
		}
		enc.putInt32(0) // throttle_time_ms
		return enc.bytes()
	}

	// v0: non-flexible, no throttle_time_ms.
	enc := newEncoder(64)
	enc.putInt16(errNone)
	enc.putArrayLen(len(supported))
	for _, entry := range supported {
		enc.putInt16(entry.APIKey)
		enc.putInt16(entry.MinVersion)
		enc.putInt16(entry.MaxVersion)
	}
	return enc.bytes()
}

// --- Metadata (key 3) ---

func (h *handler) handleMetadata(hdr requestHeader, body []byte) []byte {
	d := newDecoder(body)
	var reqTopics []string
	topicCount := d.readArrayLen()
	if topicCount >= 0 {
		reqTopics = make([]string, topicCount)
		for i := int32(0); i < topicCount; i++ {
			reqTopics[i] = d.readString()
		}
	}

	// If no topics requested, return all known topics.
	var topicNames []string
	if len(reqTopics) == 0 {
		topicNames = h.broker.topicNames()
	} else {
		topicNames = reqTopics
	}

	enc := newEncoder(256)

	// Brokers array.
	enc.putArrayLen(1)
	enc.putInt32(h.nodeID) // node_id
	enc.putString(h.host)  // host
	enc.putInt32(h.port)   // port

	// Topics array.
	enc.putArrayLen(len(topicNames))
	for _, name := range topicNames {
		t := h.broker.getTopic(name)
		if t == nil {
			// Auto-create the topic (lazy).
			t = h.broker.getOrCreateTopic(name)
		}

		enc.putInt16(errNone) // topic error_code
		enc.putString(name)   // topic name

		enc.putArrayLen(len(t.partitions))
		for i := range t.partitions {
			enc.putInt16(errNone)  // partition error_code
			enc.putInt32(int32(i)) // partition_id
			enc.putInt32(h.nodeID) // leader
			enc.putArrayLen(1)     // replicas
			enc.putInt32(h.nodeID)
			enc.putArrayLen(1) // isr
			enc.putInt32(h.nodeID)
		}
	}

	return enc.bytes()
}

// --- FindCoordinator (key 10) ---

func (h *handler) handleFindCoordinator(_ requestHeader, body []byte) []byte {
	// Always return ourselves as the coordinator.
	enc := newEncoder(32)
	enc.putInt16(errNone) // error_code
	enc.putInt32(h.nodeID)
	enc.putString(h.host)
	enc.putInt32(h.port)
	return enc.bytes()
}

// --- JoinGroup (key 11) ---

func (h *handler) handleJoinGroup(_ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := joinGroupRequest{
		GroupID:          d.readString(),
		SessionTimeoutMs: d.readInt32(),
		MemberID:         d.readString(),
		ProtocolType:     d.readString(),
	}

	protocolCount := d.readArrayLen()
	if protocolCount > 0 {
		req.Protocols = make([]joinGroupProtocol, protocolCount)
		for i := int32(0); i < protocolCount; i++ {
			req.Protocols[i].Name = d.readString()
			req.Protocols[i].Metadata = d.readBytes()
		}
	}

	if d.err != nil {
		metrics.KafkaServerProtocolErrors.Inc()
		return h.encodeJoinGroupError(errGroupAuthorizationFailed)
	}

	g := h.groups.getOrCreate(req.GroupID)
	errCode, genID, protoName, leaderID, memberID, members := g.joinGroup(req)

	enc := newEncoder(128)
	enc.putInt16(errCode)
	enc.putInt32(genID)
	enc.putString(protoName)
	enc.putString(leaderID)
	enc.putString(memberID)
	enc.putArrayLen(len(members))
	for _, m := range members {
		enc.putString(m.MemberID)
		enc.putBytes(m.Metadata)
	}

	return enc.bytes()
}

func (h *handler) encodeJoinGroupError(errCode int16) []byte {
	enc := newEncoder(32)
	enc.putInt16(errCode)
	enc.putInt32(0)    // generationID
	enc.putString("")  // protocolName
	enc.putString("")  // leaderID
	enc.putString("")  // memberID
	enc.putArrayLen(0) // members
	return enc.bytes()
}

// --- SyncGroup (key 14) ---

func (h *handler) handleSyncGroup(_ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := syncGroupRequest{
		GroupID:      d.readString(),
		GenerationID: d.readInt32(),
		MemberID:     d.readString(),
	}

	assignCount := d.readArrayLen()
	if assignCount > 0 {
		req.Assignments = make([]syncGroupAssignment, assignCount)
		for i := int32(0); i < assignCount; i++ {
			req.Assignments[i].MemberID = d.readString()
			req.Assignments[i].Assignment = d.readBytes()
		}
	}

	if d.err != nil {
		metrics.KafkaServerProtocolErrors.Inc()
		enc := newEncoder(16)
		enc.putInt16(errGroupAuthorizationFailed)
		enc.putBytes(nil)
		return enc.bytes()
	}

	g := h.groups.getOrCreate(req.GroupID)
	errCode, assignment := g.syncGroup(req)

	enc := newEncoder(32 + len(assignment))
	enc.putInt16(errCode)
	enc.putBytes(assignment)
	return enc.bytes()
}

// --- Heartbeat (key 12) ---

func (h *handler) handleHeartbeat(_ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := heartbeatRequest{
		GroupID:      d.readString(),
		GenerationID: d.readInt32(),
		MemberID:     d.readString(),
	}

	if d.err != nil {
		enc := newEncoder(4)
		enc.putInt16(errGroupAuthorizationFailed)
		return enc.bytes()
	}

	g := h.groups.getOrCreate(req.GroupID)
	errCode := g.heartbeat(req)

	enc := newEncoder(4)
	enc.putInt16(errCode)
	return enc.bytes()
}

// --- LeaveGroup (key 13) ---

func (h *handler) handleLeaveGroup(_ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := leaveGroupRequest{
		GroupID:  d.readString(),
		MemberID: d.readString(),
	}

	if d.err != nil {
		enc := newEncoder(4)
		enc.putInt16(errGroupAuthorizationFailed)
		return enc.bytes()
	}

	g := h.groups.getOrCreate(req.GroupID)
	errCode := g.leaveGroup(req)

	enc := newEncoder(4)
	enc.putInt16(errCode)
	return enc.bytes()
}

// --- ListOffsets (key 2) ---

func (h *handler) handleListOffsets(_ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	_ = d.readInt32() // replica_id (ignored)

	topicCount := d.readArrayLen()
	type partResult struct {
		partition int32
		errCode   int16
		offset    int64
		timestamp int64
	}
	type topicResult struct {
		name       string
		partitions []partResult
	}

	results := make([]topicResult, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		topicName := d.readString()
		partCount := d.readArrayLen()
		tr := topicResult{name: topicName}

		t := h.broker.getTopic(topicName)
		for j := int32(0); j < partCount; j++ {
			partID := d.readInt32()
			ts := d.readInt64() // timestamp: -2=earliest, -1=latest

			if t == nil || int(partID) >= len(t.partitions) {
				tr.partitions = append(tr.partitions, partResult{
					partition: partID,
					errCode:   errUnknownTopicOrPartition,
				})
				continue
			}

			p := t.partitions[partID]
			var offset int64
			switch ts {
			case -2: // earliest
				offset = p.oldestOffset()
			case -1: // latest
				offset = p.latestOffset()
			default:
				offset = p.latestOffset()
			}

			tr.partitions = append(tr.partitions, partResult{
				partition: partID,
				errCode:   errNone,
				offset:    offset,
				timestamp: -1,
			})
		}
		results = append(results, tr)
	}

	enc := newEncoder(128)
	enc.putArrayLen(len(results))
	for _, tr := range results {
		enc.putString(tr.name)
		enc.putArrayLen(len(tr.partitions))
		for _, pr := range tr.partitions {
			enc.putInt32(pr.partition)
			enc.putInt16(pr.errCode)
			enc.putInt64(pr.timestamp)
			enc.putInt64(pr.offset)
		}
	}
	return enc.bytes()
}

// --- Fetch (key 1) ---

func (h *handler) handleFetch(ctx context.Context, _ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := fetchRequest{
		MaxWaitMs: -1,
		MinBytes:  -1,
	}
	_ = d.readInt32() // replica_id
	req.MaxWaitMs = d.readInt32()
	req.MinBytes = d.readInt32()

	topicCount := d.readArrayLen()
	for i := int32(0); i < topicCount; i++ {
		ft := fetchRequestTopic{Topic: d.readString()}
		partCount := d.readArrayLen()
		for j := int32(0); j < partCount; j++ {
			fp := fetchRequestPartition{
				Partition:   d.readInt32(),
				FetchOffset: d.readInt64(),
				MaxBytes:    d.readInt32(),
			}
			ft.Partitions = append(ft.Partitions, fp)
		}
		req.Topics = append(req.Topics, ft)
	}

	metrics.KafkaServerFetchRequests.Inc()

	// First attempt: try to read data.
	resp, hasData := h.buildFetchResponse(req)

	// If no data and maxWaitMs > 0, do long-polling.
	if !hasData && req.MaxWaitMs > 0 {
		waitMs := req.MaxWaitMs
		if waitMs > 30000 {
			waitMs = 30000 // cap at 30s
		}

		// Collect waiters from all requested partitions.
		var waitChs []<-chan struct{}
		for _, ft := range req.Topics {
			t := h.broker.getTopic(ft.Topic)
			if t == nil {
				continue
			}
			for _, fp := range ft.Partitions {
				if int(fp.Partition) < len(t.partitions) {
					waitChs = append(waitChs, t.partitions[fp.Partition].waiter())
				}
			}
		}

		if len(waitChs) > 0 {
			// Wait for any partition to get new data or timeout.
			timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
			defer timer.Stop()

			// Create a combined channel.
			anyReady := make(chan struct{}, 1)
			for _, ch := range waitChs {
				go func(c <-chan struct{}) {
					select {
					case <-c:
						select {
						case anyReady <- struct{}{}:
						default:
						}
					case <-ctx.Done():
					}
				}(ch)
			}

			select {
			case <-anyReady:
			case <-timer.C:
			case <-ctx.Done():
			}

			// Rebuild response with new data.
			resp, _ = h.buildFetchResponse(req)
		}
	}

	return resp
}

func (h *handler) buildFetchResponse(req fetchRequest) ([]byte, bool) {
	type partResp struct {
		partition int32
		errCode   int16
		highWM    int64
		batch     []byte
	}
	type topicResp struct {
		name       string
		partitions []partResp
	}

	hasData := false
	results := make([]topicResp, 0, len(req.Topics))
	for _, ft := range req.Topics {
		tr := topicResp{name: ft.Topic}
		t := h.broker.getTopic(ft.Topic)

		for _, fp := range ft.Partitions {
			if t == nil || int(fp.Partition) >= len(t.partitions) {
				tr.partitions = append(tr.partitions, partResp{
					partition: fp.Partition,
					errCode:   errUnknownTopicOrPartition,
					highWM:    -1,
				})
				continue
			}

			p := t.partitions[fp.Partition]
			records, errCode := p.readFrom(fp.FetchOffset, fp.MaxBytes)
			highWM := p.latestOffset()

			var batch []byte
			if len(records) > 0 {
				batch = encodeRecordBatch(records)
				metrics.KafkaServerRecordsFetched.Add(float64(len(records)))
				hasData = true
			}

			tr.partitions = append(tr.partitions, partResp{
				partition: fp.Partition,
				errCode:   errCode,
				highWM:    highWM,
				batch:     batch,
			})
		}
		results = append(results, tr)
	}

	enc := newEncoder(256)
	enc.putArrayLen(len(results))
	for _, tr := range results {
		enc.putString(tr.name)
		enc.putArrayLen(len(tr.partitions))
		for _, pr := range tr.partitions {
			enc.putInt32(pr.partition)
			enc.putInt16(pr.errCode)
			enc.putInt64(pr.highWM) // high watermark

			// RecordBatch as bytes (int32 length prefix).
			if pr.batch == nil {
				enc.putInt32(-1)
			} else {
				enc.putInt32(int32(len(pr.batch)))
				enc.buf = append(enc.buf, pr.batch...)
			}
		}
	}
	return enc.bytes(), hasData
}

// --- OffsetCommit (key 8) ---

func (h *handler) handleOffsetCommit(ctx context.Context, _ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := offsetCommitRequest{
		GroupID:      d.readString(),
		GenerationID: d.readInt32(),
		MemberID:     d.readString(),
	}
	_ = d.readInt64() // retention_time_ms (v2, ignored)

	topicCount := d.readArrayLen()
	for i := int32(0); i < topicCount; i++ {
		t := offsetCommitRequestTopic{Topic: d.readString()}
		partCount := d.readArrayLen()
		for j := int32(0); j < partCount; j++ {
			p := offsetCommitRequestPartition{
				Partition: d.readInt32(),
				Offset:    d.readInt64(),
				Metadata:  d.readString(),
			}
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}

	type partResp struct {
		partition int32
		errCode   int16
	}
	type topicResp struct {
		name       string
		partitions []partResp
	}

	results := make([]topicResp, 0, len(req.Topics))
	for _, t := range req.Topics {
		tr := topicResp{name: t.Topic}
		for _, p := range t.Partitions {
			ec := errNone
			if h.offsets != nil {
				if err := h.offsets.commitOffset(ctx, req.GroupID, t.Topic, p.Partition, p.Offset); err != nil {
					h.logger.Error("offset commit failed",
						"error", err,
						"group", req.GroupID,
						"topic", t.Topic,
						"partition", p.Partition,
					)
					ec = errGroupAuthorizationFailed
				}
			}
			tr.partitions = append(tr.partitions, partResp{partition: p.Partition, errCode: ec})
			if ec == errNone {
				metrics.KafkaServerOffsetCommits.Inc()

				// Cooperative checkpoint: ack the LSN at the committed offset.
				if h.ackFn != nil {
					lsn := h.findLSNForOffset(t.Topic, p.Partition, p.Offset)
					if lsn > 0 {
						h.ackFn(lsn)
					}
				}
			}
		}
		results = append(results, tr)
	}

	enc := newEncoder(64)
	enc.putArrayLen(len(results))
	for _, tr := range results {
		enc.putString(tr.name)
		enc.putArrayLen(len(tr.partitions))
		for _, pr := range tr.partitions {
			enc.putInt32(pr.partition)
			enc.putInt16(pr.errCode)
		}
	}
	return enc.bytes()
}

// findLSNForOffset looks up the LSN stored in the record at the given offset.
func (h *handler) findLSNForOffset(topic string, partition int32, offset int64) uint64 {
	t := h.broker.getTopic(topic)
	if t == nil || int(partition) >= len(t.partitions) {
		return 0
	}
	p := t.partitions[partition]
	// Read just this one record.
	records, errCode := p.readFrom(offset, 1024*1024)
	if errCode != errNone || len(records) == 0 {
		return 0
	}
	return records[0].LSN
}

// --- OffsetFetch (key 9) ---

func (h *handler) handleOffsetFetch(ctx context.Context, _ requestHeader, body []byte) []byte {
	d := newDecoder(body)
	req := offsetFetchRequest{
		GroupID: d.readString(),
	}

	topicCount := d.readArrayLen()
	for i := int32(0); i < topicCount; i++ {
		t := offsetFetchRequestTopic{Topic: d.readString()}
		partCount := d.readArrayLen()
		for j := int32(0); j < partCount; j++ {
			t.Partitions = append(t.Partitions, d.readInt32())
		}
		req.Topics = append(req.Topics, t)
	}

	type partResp struct {
		partition int32
		offset    int64
		metadata  string
		errCode   int16
	}
	type topicResp struct {
		name       string
		partitions []partResp
	}

	results := make([]topicResp, 0, len(req.Topics))
	for _, t := range req.Topics {
		tr := topicResp{name: t.Topic}
		for _, partID := range t.Partitions {
			offset := int64(-1)
			ec := errNone
			if h.offsets != nil {
				var err error
				offset, err = h.offsets.fetchOffset(ctx, req.GroupID, t.Topic, partID)
				if err != nil {
					h.logger.Error("offset fetch failed",
						"error", err,
						"group", req.GroupID,
						"topic", t.Topic,
						"partition", partID,
					)
					ec = errGroupAuthorizationFailed
				}
			}
			tr.partitions = append(tr.partitions, partResp{
				partition: partID,
				offset:    offset,
				errCode:   ec,
			})
		}
		results = append(results, tr)
	}

	enc := newEncoder(64)
	enc.putArrayLen(len(results))
	for _, tr := range results {
		enc.putString(tr.name)
		enc.putArrayLen(len(tr.partitions))
		for _, pr := range tr.partitions {
			enc.putInt32(pr.partition)
			enc.putInt64(pr.offset)
			enc.putString(pr.metadata) // metadata
			enc.putInt16(pr.errCode)
		}
	}
	return enc.bytes()
}

func (h *handler) encodeUnsupportedVersion() []byte {
	enc := newEncoder(4)
	enc.putInt16(errUnsupportedVersion)
	return enc.bytes()
}
