package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const adapterName = "kafka"

// Adapter publishes events to a Kafka topic.
type Adapter struct {
	brokers     []string
	topic       string // fixed topic; empty = per-channel mapping
	saslMech    sasl.Mechanism
	tlsConfig   *tls.Config
	dlqInstance dlq.DLQ
	ackFn       adapter.AckFunc
	backoffBase time.Duration
	backoffCap  time.Duration
	logger      *slog.Logger
}

// SetDLQ implements pgcdc.DLQAware.
func (a *Adapter) SetDLQ(d dlq.DLQ) { a.dlqInstance = d }

// SetAckFunc implements adapter.Acknowledger.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

// Name returns the adapter name.
func (a *Adapter) Name() string { return adapterName }

// New creates a Kafka adapter. Duration parameters default to sensible values
// when zero.
func New(
	brokers []string,
	topic, saslMechanism, saslUser, saslPass, tlsCAFile string,
	tlsEnabled bool,
	backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	if backoffBase <= 0 {
		backoffBase = 1 * time.Second
	}
	if backoffCap <= 0 {
		backoffCap = 30 * time.Second
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	var saslMech sasl.Mechanism
	switch saslMechanism {
	case "plain":
		saslMech = plain.Mechanism{Username: saslUser, Password: saslPass}
	case "scram-sha-256":
		if m, err := scram.Mechanism(scram.SHA256, saslUser, saslPass); err == nil {
			saslMech = m
		}
	case "scram-sha-512":
		if m, err := scram.Mechanism(scram.SHA512, saslUser, saslPass); err == nil {
			saslMech = m
		}
	}

	var tlsCfg *tls.Config
	if tlsEnabled {
		tlsCfg = &tls.Config{}
		if tlsCAFile != "" {
			pem, err := os.ReadFile(tlsCAFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(pem) {
					tlsCfg.RootCAs = pool
				}
			}
		}
	}

	return &Adapter{
		brokers:     brokers,
		topic:       topic,
		saslMech:    saslMech,
		tlsConfig:   tlsCfg,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		logger:      logger.With("adapter", adapterName),
	}
}

// Start connects to Kafka and publishes events from the channel. It blocks
// until ctx is cancelled. On connection error, it reconnects with exponential
// backoff.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	var attempt int
	for {
		err := a.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("kafka connection lost, reconnecting",
			"error", err,
			"attempt", attempt+1,
			"delay", delay,
		)
		metrics.KafkaErrors.Add(1)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	transport := &kafkago.Transport{}
	if a.saslMech != nil {
		transport.SASL = a.saslMech
	}
	if a.tlsConfig != nil {
		transport.TLS = a.tlsConfig
	}

	w := &kafkago.Writer{
		Addr:         kafkago.TCP(a.brokers...),
		RequiredAcks: kafkago.RequireAll,
		Transport:    transport,
		Logger: kafkago.LoggerFunc(func(msg string, args ...interface{}) {
			a.logger.Debug(fmt.Sprintf(msg, args...))
		}),
		ErrorLogger: kafkago.LoggerFunc(func(msg string, args ...interface{}) {
			a.logger.Warn(fmt.Sprintf(msg, args...))
		}),
	}
	defer func() { _ = w.Close() }()

	a.logger.Info("kafka writer ready", "brokers", a.brokers)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}

			topic := a.topicForEvent(ev)
			msg := kafkago.Message{
				Topic: topic,
				Key:   []byte(ev.ID),
				Value: ev.Payload,
				Headers: []kafkago.Header{
					{Key: "pgcdc-channel", Value: []byte(ev.Channel)},
					{Key: "pgcdc-operation", Value: []byte(ev.Operation)},
					{Key: "pgcdc-event-id", Value: []byte(ev.ID)},
				},
			}

			start := time.Now()
			err := w.WriteMessages(ctx, msg)
			metrics.KafkaPublishDuration.Observe(time.Since(start).Seconds())

			if err != nil {
				metrics.KafkaErrors.Add(1)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if isTerminalError(err) {
					a.logger.Warn("kafka terminal write error, recording to DLQ",
						"error", err,
						"event_id", ev.ID,
						"topic", topic,
					)
					if a.dlqInstance != nil {
						_ = a.dlqInstance.Record(ctx, ev, adapterName, err)
					}
					if a.ackFn != nil && ev.LSN > 0 {
						a.ackFn(ev.LSN)
					}
					continue
				}
				return fmt.Errorf("write message: %w", err)
			}

			metrics.KafkaPublished.Add(1)
			metrics.EventsDelivered.WithLabelValues(adapterName).Inc()
			if a.ackFn != nil && ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}
}

// topicForEvent returns the Kafka topic for an event.
// If a fixed topic is configured, it is always used.
// Otherwise the channel is mapped: "pgcdc:orders" → "pgcdc.orders".
func (a *Adapter) topicForEvent(ev event.Event) string {
	if a.topic != "" {
		return a.topic
	}
	return strings.ReplaceAll(ev.Channel, ":", ".")
}

// isTerminalError returns true if the error indicates a permanent failure that
// reconnecting won't fix (e.g. authorization denied, message too large).
// These events are sent to the DLQ rather than triggering a reconnect loop.
func isTerminalError(err error) bool {
	// WriteErrors wraps per-message errors; unwrap and check each one.
	if we, ok := err.(kafkago.WriteErrors); ok {
		for _, e := range we {
			if e != nil && isTerminalError(e) {
				return true
			}
		}
		return false
	}
	var ke kafkago.Error
	if errors.As(err, &ke) {
		return !ke.Temporary()
	}
	return false
}
