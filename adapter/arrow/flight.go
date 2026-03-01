//go:build !no_arrow

package arrow

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ticketPayload is the JSON structure encoded in Flight Tickets.
type ticketPayload struct {
	Channel string `json:"channel"`
	Offset  int    `json:"offset"`
}

// flightServer implements the Arrow Flight RPC service.
type flightServer struct {
	flight.BaseFlightServer
	adapter *Adapter
}

// ListFlights returns available channels as FlightInfo entries.
func (s *flightServer) ListFlights(_ *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	names := s.adapter.channelNames()
	sort.Strings(names)

	mem := memory.DefaultAllocator
	sc := DefaultEventSchema()

	for _, name := range names {
		count := s.adapter.bufferCount(name)
		tkt, _ := json.Marshal(ticketPayload{Channel: name, Offset: 0})
		info := &flight.FlightInfo{
			Schema: flight.SerializeSchema(sc, mem),
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{name},
			},
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: tkt}},
			},
			TotalRecords: int64(count),
			TotalBytes:   -1,
		}
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

// GetFlightInfo returns schema and metadata for a specific channel.
func (s *flightServer) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if desc == nil || len(desc.Path) == 0 {
		return nil, status.Error(codes.InvalidArgument, "flight descriptor path required")
	}
	channel := desc.Path[0]
	count := s.adapter.bufferCount(channel)

	sc := s.schemaForChannel(channel)
	mem := memory.DefaultAllocator

	tkt, _ := json.Marshal(ticketPayload{Channel: channel, Offset: 0})
	return &flight.FlightInfo{
		Schema: flight.SerializeSchema(sc, mem),
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{channel},
		},
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: tkt}},
		},
		TotalRecords: int64(count),
		TotalBytes:   -1,
	}, nil
}

// DoGet streams events as Arrow RecordBatches for a given ticket.
func (s *flightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	var tp ticketPayload
	if err := json.Unmarshal(ticket.GetTicket(), &tp); err != nil {
		metrics.ArrowFlightErrors.Inc()
		return status.Errorf(codes.InvalidArgument, "parse ticket: %v", err)
	}
	if tp.Channel == "" {
		metrics.ArrowFlightErrors.Inc()
		return status.Error(codes.InvalidArgument, "channel required in ticket")
	}

	events := s.adapter.getEvents(tp.Channel, tp.Offset)
	if len(events) == 0 {
		sc := s.schemaForChannel(tp.Channel)
		w := flight.NewRecordWriter(stream, ipc.WithSchema(sc))
		rec := s.buildEmptyBatch(sc)
		defer rec.Release()
		err := w.Write(rec)
		if closeErr := w.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		return err
	}

	metrics.ArrowFlightClients.Inc()
	defer metrics.ArrowFlightClients.Dec()

	sc := s.schemaForChannel(tp.Channel)
	w := flight.NewRecordWriter(stream, ipc.WithSchema(sc))

	rec, err := s.buildRecordBatch(sc, events)
	if err != nil {
		_ = w.Close()
		metrics.ArrowFlightErrors.Inc()
		return status.Errorf(codes.Internal, "build record batch: %v", err)
	}
	defer rec.Release()

	if err := w.Write(rec); err != nil {
		_ = w.Close()
		metrics.ArrowFlightErrors.Inc()
		return err
	}
	metrics.ArrowFlightRecordsSent.Add(float64(len(events)))
	return w.Close()
}

// schemaForChannel returns the Arrow schema for a channel, using the schema
// store if available, otherwise the default event schema.
func (s *flightServer) schemaForChannel(channel string) *arrow.Schema {
	if s.adapter.schemaStore != nil {
		if sc := BuildSchemaFromStore(s.adapter.schemaStore, channel); sc != nil {
			return sc
		}
	}
	return DefaultEventSchema()
}

// buildRecordBatch creates a RecordBatch from events using the event schema.
// When the schema has exactly 6 fields (default event schema), uses the typed
// field builder. For custom schemas from the schema store, falls back to the
// default event schema to avoid field count mismatch panics.
func (s *flightServer) buildRecordBatch(sc *arrow.Schema, events []event.Event) (arrow.RecordBatch, error) {
	if sc.NumFields() != 6 {
		// Custom schema from store — use default event schema for record building
		// since we don't have structured column data from the event payload.
		sc = DefaultEventSchema()
	}
	mem := memory.DefaultAllocator
	b := array.NewRecordBuilder(mem, sc)
	defer b.Release()

	return s.buildDefaultBatch(b, events)
}

// buildDefaultBatch builds a record batch using the default 6-column event schema.
func (s *flightServer) buildDefaultBatch(b *array.RecordBuilder, events []event.Event) (arrow.RecordBatch, error) {
	for _, ev := range events {
		ev.EnsurePayload()
		b.Field(0).(*array.StringBuilder).Append(ev.ID)
		b.Field(1).(*array.StringBuilder).Append(ev.Channel)
		b.Field(2).(*array.StringBuilder).Append(ev.Operation)
		b.Field(3).(*array.StringBuilder).Append(string(ev.Payload))
		b.Field(4).(*array.StringBuilder).Append(ev.Source)
		b.Field(5).(*array.TimestampBuilder).Append(arrow.Timestamp(ev.CreatedAt.UnixMicro()))
	}
	return b.NewRecordBatch(), nil
}

// buildEmptyBatch creates an empty RecordBatch with the given schema.
func (s *flightServer) buildEmptyBatch(sc *arrow.Schema) arrow.RecordBatch {
	mem := memory.DefaultAllocator
	b := array.NewRecordBuilder(mem, sc)
	defer b.Release()
	return b.NewRecordBatch()
}
