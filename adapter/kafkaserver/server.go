package kafkaserver

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const adapterName = "kafkaserver"

// Adapter implements adapter.Adapter and adapter.Acknowledger.
// It starts a TCP server speaking the Kafka wire protocol, allowing any
// Kafka consumer library to connect and consume CDC events directly.
type Adapter struct {
	addr           string
	partitionCount int
	bufferSize     int
	sessionTimeout time.Duration
	keyColumn      string
	cpStore        checkpoint.Store
	logger         *slog.Logger
	ackFn          adapter.AckFunc
	activeConns    atomic.Int64
}

// New creates a Kafka protocol server adapter.
func New(addr string, partitionCount, bufferSize int, sessionTimeout time.Duration, keyColumn string, cpStore checkpoint.Store, logger *slog.Logger) *Adapter {
	if addr == "" {
		addr = ":9092"
	}
	if partitionCount <= 0 {
		partitionCount = 8
	}
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	if sessionTimeout <= 0 {
		sessionTimeout = 30 * time.Second
	}
	if keyColumn == "" {
		keyColumn = "id"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		addr:           addr,
		partitionCount: partitionCount,
		bufferSize:     bufferSize,
		sessionTimeout: sessionTimeout,
		keyColumn:      keyColumn,
		cpStore:        cpStore,
		logger:         logger.With("adapter", adapterName),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string { return adapterName }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// Start consumes events from the bus and serves them over the Kafka protocol.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("kafkaserver adapter starting", "addr", a.addr)

	// Parse the listen address to extract host and port for Metadata responses.
	host, port := parseAddr(a.addr)

	brk := newBroker(a.partitionCount, a.bufferSize, a.keyColumn)
	groups := newGroupCoordinator(a.sessionTimeout, a.logger)

	var offsets *offsetStore
	if a.cpStore != nil {
		offsets = newOffsetStore(a.cpStore, a.logger)
	}

	h := &handler{
		broker:  brk,
		groups:  groups,
		offsets: offsets,
		nodeID:  0,
		host:    host,
		port:    port,
		ackFn:   a.ackFn,
		logger:  a.logger,
	}

	ln, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("kafkaserver listen: %w", err)
	}

	done := make(chan struct{})

	// Start session reaper.
	groups.startReaper(done)

	// Ingest goroutine: read events from bus and route to broker.
	go func() {
		for ev := range events {
			brk.ingest(ev)
		}
	}()

	// Accept loop.
	var wg sync.WaitGroup
	go func() {
		acceptBackoff := 10 * time.Millisecond
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				select {
				case <-done:
					return
				default:
					a.logger.Error("accept error", "error", acceptErr)
					// Exponential backoff on persistent accept errors.
					time.Sleep(acceptBackoff)
					acceptBackoff *= 2
					if acceptBackoff > time.Second {
						acceptBackoff = time.Second
					}
					continue
				}
			}

			acceptBackoff = 10 * time.Millisecond

			wg.Add(1)
			a.activeConns.Add(1)
			metrics.KafkaServerConnectionsActive.Set(float64(a.activeConns.Load()))

			go func() {
				defer wg.Done()
				defer func() {
					a.activeConns.Add(-1)
					metrics.KafkaServerConnectionsActive.Set(float64(a.activeConns.Load()))
				}()
				a.handleConn(ctx, conn, h)
			}()
		}
	}()

	// Wait for context cancellation.
	<-ctx.Done()
	close(done)
	_ = ln.Close()
	wg.Wait()

	a.logger.Info("kafkaserver adapter stopped")
	return ctx.Err()
}

// handleConn processes Kafka protocol requests on a single TCP connection.
func (a *Adapter) handleConn(ctx context.Context, conn net.Conn, h *handler) {
	defer func() { _ = conn.Close() }()

	r := bufio.NewReaderSize(conn, 64*1024)
	w := bufio.NewWriterSize(conn, 64*1024)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set a read deadline to allow periodic ctx checks.
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))

		hdr, body, err := readRequest(r)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			// Connection closed or protocol error.
			return
		}

		resp := h.dispatch(ctx, hdr, body)
		if err := writeResponse(w, hdr.CorrelationID, resp); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

// parseAddr extracts host and port from a listen address.
func parseAddr(addr string) (string, int32) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "localhost", 9092
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	var port int
	for _, c := range portStr {
		if c >= '0' && c <= '9' {
			port = port*10 + int(c-'0')
		}
	}
	if port == 0 {
		port = 9092
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return host, int32(port)
}
