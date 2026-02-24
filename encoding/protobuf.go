package encoding

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/florinutz/pgcdc/encoding/registry"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	pbplugin "github.com/florinutz/pgcdc/plugin/proto"
)

// ProtobufEncoder encodes events using the pgcdc protobuf Event message.
type ProtobufEncoder struct {
	registry   *registry.Client
	registryID int // cached schema ID (single schema for all events)
	logger     *slog.Logger
}

// NewProtobufEncoder creates a Protobuf encoder.
func NewProtobufEncoder(reg *registry.Client, logger *slog.Logger) *ProtobufEncoder {
	if logger == nil {
		logger = slog.Default()
	}
	return &ProtobufEncoder{
		registry: reg,
		logger:   logger.With("component", "protobuf_encoder"),
	}
}

func (e *ProtobufEncoder) ContentType() string { return "application/x-protobuf" }

func (e *ProtobufEncoder) Close() error { return nil }

// Encode converts an event.Event into a protobuf-encoded message.
// The data parameter is the original payload (used as the Payload bytes field).
func (e *ProtobufEncoder) Encode(ev event.Event, data []byte) ([]byte, error) {
	pbEvent := &pbplugin.Event{
		Id:              ev.ID,
		Channel:         ev.Channel,
		Operation:       ev.Operation,
		Payload:         data,
		Source:          ev.Source,
		CreatedAtUnixNs: ev.CreatedAt.UnixNano(),
	}
	if ev.Transaction != nil {
		pbEvent.Transaction = &pbplugin.TransactionInfo{
			Xid:              ev.Transaction.Xid,
			CommitTimeUnixNs: ev.Transaction.CommitTime.UnixNano(),
			Seq:              int32(ev.Transaction.Seq),
		}
	}

	encoded, err := proto.Marshal(pbEvent)
	if err != nil {
		metrics.EncodingErrors.Inc()
		return nil, fmt.Errorf("protobuf marshal: %w", err)
	}

	metrics.EncodingEncoded.Inc()

	if e.registry != nil {
		schemaID, regErr := e.ensureRegistered(ev)
		if regErr != nil {
			return nil, regErr
		}
		return registry.WireEncodeProtobuf(schemaID, encoded), nil
	}

	return encoded, nil
}

// ensureRegistered registers the protobuf schema with the registry if not already done.
func (e *ProtobufEncoder) ensureRegistered(ev event.Event) (int, error) {
	if e.registryID > 0 {
		return e.registryID, nil
	}

	// Use the .proto file content as the schema.
	protoSchema := `syntax = "proto3";
package pgcdc.plugin.v1;

message Event {
  string id = 1;
  string channel = 2;
  string operation = 3;
  bytes payload = 4;
  string source = 5;
  int64 created_at_unix_ns = 6;
  TransactionInfo transaction = 7;
}

message TransactionInfo {
  uint32 xid = 1;
  int64 commit_time_unix_ns = 2;
  int32 seq = 3;
}`

	subject := topicForEvent(ev) + "-value"
	id, err := e.registry.Register(context.Background(), subject, protoSchema, registry.SchemaTypeProtobuf)
	if err != nil {
		metrics.SchemaRegistryErrors.Inc()
		return 0, fmt.Errorf("register protobuf schema: %w", err)
	}
	metrics.SchemaRegistryRegistrations.Inc()
	e.registryID = id
	return id, nil
}
