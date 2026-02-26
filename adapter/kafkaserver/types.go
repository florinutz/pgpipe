package kafkaserver

// Kafka protocol API keys.
const (
	apiFetch           int16 = 1
	apiListOffsets     int16 = 2
	apiMetadata        int16 = 3
	apiOffsetCommit    int16 = 8
	apiOffsetFetch     int16 = 9
	apiFindCoordinator int16 = 10
	apiJoinGroup       int16 = 11
	apiHeartbeat       int16 = 12
	apiLeaveGroup      int16 = 13
	apiSyncGroup       int16 = 14
	apiApiVersions     int16 = 18
)

// Error codes.
const (
	errNone                     int16 = 0
	errOffsetOutOfRange         int16 = 1
	errUnknownTopicOrPartition  int16 = 3
	errRebalanceInProgress      int16 = 27
	errIllegalGeneration        int16 = 22
	errUnknownMemberID          int16 = 25
	errUnsupportedVersion       int16 = 35
	errGroupAuthorizationFailed int16 = 30
	errInvalidRequest           int16 = 42
)

// requestHeader is the common header for all Kafka requests.
type requestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// apiVersionEntry describes a supported API key range.
type apiVersionEntry struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// --- Fetch ---

type fetchRequest struct {
	MaxWaitMs int32
	MinBytes  int32
	Topics    []fetchRequestTopic
}

type fetchRequestTopic struct {
	Topic      string
	Partitions []fetchRequestPartition
}

type fetchRequestPartition struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

// --- JoinGroup ---

type joinGroupRequest struct {
	GroupID          string
	SessionTimeoutMs int32
	MemberID         string
	ProtocolType     string
	Protocols        []joinGroupProtocol
}

type joinGroupProtocol struct {
	Name     string
	Metadata []byte
}

type joinGroupResponseMember struct {
	MemberID string
	Metadata []byte
}

// --- SyncGroup ---

type syncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []syncGroupAssignment
}

type syncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// --- Heartbeat ---

type heartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

// --- LeaveGroup ---

type leaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// --- OffsetCommit ---

type offsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Topics       []offsetCommitRequestTopic
}

type offsetCommitRequestTopic struct {
	Topic      string
	Partitions []offsetCommitRequestPartition
}

type offsetCommitRequestPartition struct {
	Partition int32
	Offset    int64
	Metadata  string
}

// --- OffsetFetch ---

type offsetFetchRequest struct {
	GroupID string
	Topics  []offsetFetchRequestTopic
}

type offsetFetchRequestTopic struct {
	Topic      string
	Partitions []int32
}

// Record stored in the ring buffer.
type record struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []recordHeader
	Timestamp int64 // unix milliseconds
	LSN       uint64
}

type recordHeader struct {
	Key   string
	Value []byte
}
