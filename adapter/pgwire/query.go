//go:build !no_pgwire

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/florinutz/pgcdc/metrics"
)

// handleQuery is the psql-wire ParseFn that routes SQL queries to the
// appropriate handler.
func (a *Adapter) handleQuery(ctx context.Context, query string) (wire.PreparedStatements, error) {
	metrics.PGWireQueriesTotal.Inc()

	q := strings.TrimSpace(query)
	q = strings.TrimRight(q, ";")
	lower := strings.ToLower(q)

	switch {
	case isEventsQuery(lower):
		return a.handleEventsQuery(ctx, lower)
	case isChannelsQuery(lower):
		return a.handleChannelsQuery(ctx)
	case isStatusQuery(lower):
		return a.handleStatusQuery(ctx)
	default:
		return a.handleUnknownQuery(ctx, q)
	}
}

// isEventsQuery checks if the query targets pgcdc_events.
func isEventsQuery(lower string) bool {
	return strings.Contains(lower, "pgcdc_events")
}

// isChannelsQuery checks if the query targets pgcdc_channels.
func isChannelsQuery(lower string) bool {
	return strings.Contains(lower, "pgcdc_channels")
}

// isStatusQuery checks if the query targets pgcdc_status.
func isStatusQuery(lower string) bool {
	return strings.Contains(lower, "pgcdc_status")
}

// handleEventsQuery returns buffered events, optionally filtered by channel.
func (a *Adapter) handleEventsQuery(_ context.Context, lower string) (wire.PreparedStatements, error) {
	channelFilter := extractChannelFilter(lower)

	handle := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		events := a.snapshot()
		count := 0
		for _, ev := range events {
			if channelFilter != "" && ev.Channel != channelFilter {
				continue
			}
			lsn := fmt.Sprintf("%X/%X", ev.LSN>>32, ev.LSN&0xFFFFFFFF)
			if ev.LSN == 0 {
				lsn = "0/0"
			}
			if err := writer.Row([]any{
				ev.ID,
				ev.Channel,
				ev.Operation,
				ev.Source,
				string(ev.Payload),
				ev.CreatedAt,
				lsn,
			}); err != nil {
				return err
			}
			count++
		}
		return writer.Complete(fmt.Sprintf("SELECT %d", count))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(eventsColumns))), nil
}

// handleChannelsQuery returns unique channels from the buffer with event counts.
func (a *Adapter) handleChannelsQuery(_ context.Context) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		events := a.snapshot()
		counts := make(map[string]int32)
		order := make([]string, 0)
		for _, ev := range events {
			if _, exists := counts[ev.Channel]; !exists {
				order = append(order, ev.Channel)
			}
			counts[ev.Channel]++
		}
		for _, ch := range order {
			if err := writer.Row([]any{ch, counts[ch]}); err != nil {
				return err
			}
		}
		return writer.Complete(fmt.Sprintf("SELECT %d", len(order)))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(channelsColumns))), nil
}

// handleStatusQuery returns pipeline status information.
func (a *Adapter) handleStatusQuery(_ context.Context) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		a.mu.RLock()
		count := a.count
		bufSize := a.bufferSize
		a.mu.RUnlock()

		buffered := count
		if buffered > bufSize {
			buffered = bufSize
		}

		rows := [][]any{
			{"adapter", "pgwire"},
			{"buffer_capacity", fmt.Sprintf("%d", bufSize)},
			{"buffer_used", fmt.Sprintf("%d", buffered)},
			{"events_total", fmt.Sprintf("%d", count)},
			{"server_time", time.Now().UTC().Format(time.RFC3339)},
		}

		for _, row := range rows {
			if err := writer.Row(row); err != nil {
				return err
			}
		}
		return writer.Complete(fmt.Sprintf("SELECT %d", len(rows)))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(statusColumns))), nil
}

// handleUnknownQuery returns an error for unrecognized queries.
func (a *Adapter) handleUnknownQuery(_ context.Context, query string) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		return writer.Complete("SELECT 0")
	}

	a.logger.Debug("unrecognized query", "query", query)
	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(statusColumns))), nil
}

// extractChannelFilter extracts a channel name from a WHERE clause.
// Supports: WHERE channel = 'xxx' (simple string matching, not full SQL parsing).
func extractChannelFilter(lower string) string {
	idx := strings.Index(lower, "where")
	if idx < 0 {
		return ""
	}
	where := lower[idx:]

	// Look for channel = 'value' pattern.
	patterns := []string{"channel = '", "channel='"}
	for _, p := range patterns {
		start := strings.Index(where, p)
		if start < 0 {
			continue
		}
		valStart := start + len(p)
		rest := where[valStart:]
		end := strings.IndexByte(rest, '\'')
		if end < 0 {
			continue
		}
		return rest[:end]
	}
	return ""
}
