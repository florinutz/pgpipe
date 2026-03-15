package walreplication

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/florinutz/pgcdc/detector/walreplication/toastcache"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/snapshot"
)

// txState tracks the current transaction when txMetadata is enabled.
type txState struct {
	xid        uint32
	commitTime time.Time
	seq        int
}

// streamMessages runs the WAL message loop, processing replication messages
// and emitting events until ctx is cancelled or an error occurs.
func (d *Detector) streamMessages(ctx context.Context, conn *pgconn.PgConn, events chan<- event.Event, consistentPoint pglogrepl.LSN, slotName string, tc *toastcache.Cache) error {
	clientXLogPos := consistentPoint
	nextStatusDeadline := time.Now().Add(standbyStatusInterval)
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	channelNames := make(map[uint32]string) // cached channel name per relation
	var currentTx *txState                  // non-nil during a transaction when txMetadata is enabled

	for {
		if time.Now().After(nextStatusDeadline) {
			// Use cooperative LSN (min acked by all adapters) if available,
			// otherwise use the detector's own receive position.
			reportLSN := clientXLogPos
			if d.cooperativeLSNFn != nil {
				coopLSN := pglogrepl.LSN(d.cooperativeLSNFn())
				if coopLSN > 0 && coopLSN <= clientXLogPos {
					reportLSN = coopLSN
				} else {
					// No adapter has acked yet — don't advance the checkpoint
					// or standby position until at least one ack arrives.
					reportLSN = 0
				}
			}

			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: reportLSN})
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}

			// Checkpoint after successful standby status update.
			if d.checkpointStore != nil && reportLSN > 0 {
				if saveErr := d.checkpointStore.Save(ctx, slotName, uint64(reportLSN)); saveErr != nil {
					d.logger.Warn("failed to save checkpoint", "error", saveErr)
				} else {
					metrics.CheckpointLSN.Set(float64(reportLSN))
					metrics.CheckpointTotal.Inc()
				}
			}

			nextStatusDeadline = time.Now().Add(standbyStatusInterval)
		}

		// Backpressure: pause in red zone (block until resumed).
		if d.backpressureCtrl != nil && d.backpressureCtrl.IsPaused() {
			d.logger.Warn("backpressure: detector paused, waiting for WAL lag to decrease")
			if err := d.backpressureCtrl.WaitResume(ctx); err != nil {
				return err
			}
			d.logger.Info("backpressure: detector resumed")
		}
		// Backpressure: throttle in yellow zone (context-aware sleep).
		if d.backpressureCtrl != nil {
			if throttle := d.backpressureCtrl.ThrottleDuration(); throttle > 0 {
				metrics.BackpressureThrottleDuration.Observe(throttle.Seconds())
				select {
				case <-time.After(throttle):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStatusDeadline)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if receiveCtx.Err() != nil && ctx.Err() == nil {
				// Deadline hit for status update, not a real error.
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres error: %s (code %s)", errMsg.Message, errMsg.Code)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				d.logger.Warn("parse keepalive failed", "error", err)
				continue
			}
			if pkm.ReplyRequested {
				nextStatusDeadline = time.Time{} // force immediate status on next iteration
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				d.logger.Warn("parse xlog data failed", "error", err)
				continue
			}

			newPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if newPos > clientXLogPos {
				clientXLogPos = newPos
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				d.logger.Warn("parse logical message failed", "error", err)
				continue
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				if oldRel, exists := relations[m.RelationID]; exists {
					if changes := diffRelation(oldRel, m); len(changes) > 0 {
						if d.schemaEvents {
							if err := d.emitSchemaChange(ctx, events, m, changes, clientXLogPos); err != nil {
								return fmt.Errorf("emit schema change: %w", err)
							}
						}
						// Evict cached rows for this relation on schema change —
						// column layout changed so cached rows may be stale.
						if tc != nil {
							tc.EvictRelation(m.RelationID)
						}
					}
				}
				relations[m.RelationID] = m
				channelNames[m.RelationID] = channelName(m)

			case *pglogrepl.BeginMessage:
				if d.txMetadata {
					currentTx = &txState{xid: m.Xid, commitTime: m.CommitTime}
				}
				if d.txMarkers {
					if err := d.emitMarker(ctx, events, "BEGIN", m.Xid, m.CommitTime, 0, clientXLogPos); err != nil {
						return err
					}
				}

			case *pglogrepl.CommitMessage:
				if d.txMarkers && currentTx != nil {
					if err := d.emitMarker(ctx, events, "COMMIT", currentTx.xid, currentTx.commitTime, currentTx.seq, clientXLogPos); err != nil {
						return err
					}
				}
				currentTx = nil

			case *pglogrepl.InsertMessage:
				if currentTx != nil {
					currentTx.seq++
				}
				if err := d.emitEvent(ctx, events, relations, m.RelationID, channelNames[m.RelationID], "INSERT", m.Tuple, nil, currentTx, clientXLogPos, tc); err != nil {
					return err
				}
				if d.incrementalEnabled {
					if rel, ok := relations[m.RelationID]; ok && rel.RelationName == d.signalTable {
						d.handleSignal(ctx, events, rel, m.Tuple)
					}
				}

			case *pglogrepl.UpdateMessage:
				if currentTx != nil {
					currentTx.seq++
				}
				if err := d.emitEvent(ctx, events, relations, m.RelationID, channelNames[m.RelationID], "UPDATE", m.NewTuple, m.OldTuple, currentTx, clientXLogPos, tc); err != nil {
					return err
				}

			case *pglogrepl.DeleteMessage:
				if currentTx != nil {
					currentTx.seq++
				}
				if err := d.emitEvent(ctx, events, relations, m.RelationID, channelNames[m.RelationID], "DELETE", nil, m.OldTuple, currentTx, clientXLogPos, tc); err != nil {
					return err
				}

			case *pglogrepl.TruncateMessage:
				for _, relID := range m.RelationIDs {
					if tc != nil {
						tc.EvictRelation(relID)
					}
					if currentTx != nil {
						currentTx.seq++
					}
					if err := d.emitEvent(ctx, events, relations, relID, channelNames[relID], "TRUNCATE", nil, nil, currentTx, clientXLogPos, tc); err != nil {
						return err
					}
				}

			case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
				// Ignored.
			}
		}
	}
}

// runHeartbeat periodically updates the heartbeat table to keep the replication
// slot advancing on idle databases. Uses a separate connection.
func (d *Detector) runHeartbeat(ctx context.Context) {
	dbURL := d.heartbeatDBURL
	if dbURL == "" {
		dbURL = d.dbURL
	}

	ticker := time.NewTicker(d.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.heartbeatOnce(ctx, dbURL); err != nil {
				d.logger.Warn("heartbeat failed", "error", err)
			}
		}
	}
}

func (d *Detector) heartbeatOnce(ctx context.Context, dbURL string) error {
	hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(hbCtx, dbURL)
	if err != nil {
		return fmt.Errorf("heartbeat connect: %w", err)
	}
	defer func() { _ = conn.Close(hbCtx) }()

	table := pgx.Identifier{d.heartbeatTable}.Sanitize()

	// Auto-create heartbeat table.
	_, err = conn.Exec(hbCtx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY DEFAULT 1,
			last_beat TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`, table))
	if err != nil {
		return fmt.Errorf("create heartbeat table: %w", err)
	}

	// Upsert heartbeat row.
	_, err = conn.Exec(hbCtx, fmt.Sprintf(`
		INSERT INTO %s (id, last_beat) VALUES (1, now())
		ON CONFLICT (id) DO UPDATE SET last_beat = now()
	`, table))
	if err != nil {
		return fmt.Errorf("heartbeat upsert: %w", err)
	}

	return nil
}

// handleSignal parses a signal table INSERT and dispatches the appropriate action.
func (d *Detector) handleSignal(ctx context.Context, events chan<- event.Event, rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) {
	row, _ := tupleToMap(rel, tuple)

	signal, _ := row["signal"].(string)
	payloadStr, _ := row["payload"].(string)

	var payload map[string]any
	if payloadStr != "" {
		if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
			d.logger.Warn("failed to parse signal payload", "signal", signal, "error", err)
			return
		}
	}

	table, _ := payload["table"].(string)
	if table == "" {
		d.logger.Warn("signal missing table in payload", "signal", signal)
		return
	}

	switch signal {
	case "execute-snapshot":
		snapshotID, _ := payload["snapshot_id"].(string)
		if snapshotID != "" {
			d.startIncrementalSnapshotWithID(ctx, events, table, snapshotID)
		} else {
			d.startIncrementalSnapshot(ctx, events, table)
		}
	case "stop-snapshot":
		d.stopIncrementalSnapshot(table)
	default:
		d.logger.Warn("unknown signal type", "signal", signal)
	}
}

// startIncrementalSnapshot starts a new incremental snapshot goroutine for the given table.
func (d *Detector) startIncrementalSnapshot(ctx context.Context, events chan<- event.Event, table string) {
	id, err := uuid.NewV7()
	if err != nil {
		d.logger.Error("generate snapshot id", "error", err)
		return
	}
	d.startIncrementalSnapshotWithID(ctx, events, table, id.String())
}

// startIncrementalSnapshotWithID starts an incremental snapshot with a specific ID.
func (d *Detector) startIncrementalSnapshotWithID(ctx context.Context, events chan<- event.Event, table, snapshotID string) {
	d.snapshotMu.Lock()
	if _, active := d.activeSnapshots[table]; active {
		d.snapshotMu.Unlock()
		d.logger.Warn("incremental snapshot already active for table, ignoring", "table", table)
		return
	}

	snapCtx, cancel := context.WithCancel(ctx)
	d.activeSnapshots[table] = cancel
	d.snapshotMu.Unlock()

	d.logger.Info("starting incremental snapshot",
		"table", table,
		"snapshot_id", snapshotID,
	)

	go func() {
		defer func() {
			d.snapshotMu.Lock()
			delete(d.activeSnapshots, table)
			d.snapshotMu.Unlock()
		}()

		snap := snapshot.NewIncremental(d.dbURL, table, d.snapshotChunkSize, d.snapshotChunkDelay, d.progressStore, snapshotID, d.logger)
		if err := snap.Run(snapCtx, events); err != nil {
			if snapCtx.Err() == nil {
				d.logger.Error("incremental snapshot failed",
					"table", table,
					"snapshot_id", snapshotID,
					"error", &pgcdcerr.IncrementalSnapshotError{
						SnapshotID: snapshotID,
						Table:      table,
						Err:        err,
					},
				)
			}
		}
	}()
}

// stopIncrementalSnapshot cancels a running incremental snapshot for the given table.
func (d *Detector) stopIncrementalSnapshot(table string) {
	d.snapshotMu.Lock()
	cancel, ok := d.activeSnapshots[table]
	d.snapshotMu.Unlock()

	if !ok {
		d.logger.Warn("no active snapshot to stop", "table", table)
		return
	}

	d.logger.Info("stopping incremental snapshot", "table", table)
	cancel()
}

// resumeIncompleteSnapshots restarts any snapshots that were running or paused
// when the detector last shut down.
func (d *Detector) resumeIncompleteSnapshots(ctx context.Context, events chan<- event.Event) {
	for _, status := range []snapshot.SnapshotStatus{snapshot.StatusRunning, snapshot.StatusPaused} {
		records, err := d.progressStore.List(ctx, &status)
		if err != nil {
			d.logger.Warn("failed to list incomplete snapshots", "status", status, "error", err)
			continue
		}
		for _, rec := range records {
			d.logger.Info("resuming incomplete snapshot",
				"snapshot_id", rec.SnapshotID,
				"table", rec.TableName,
				"status", rec.Status,
				"rows_processed", rec.RowsProcessed,
			)
			d.startIncrementalSnapshotWithID(ctx, events, rec.TableName, rec.SnapshotID)
		}
	}
}
