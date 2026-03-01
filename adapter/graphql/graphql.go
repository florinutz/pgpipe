package graphql

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/schema"
	"github.com/go-chi/chi/v5"
)

// Adapter implements a GraphQL subscriptions adapter using the
// graphql-transport-ws protocol over WebSocket. It follows the broker
// pattern: maintains a set of connected clients, each with their own
// subscriptions, and broadcasts matching events.
type Adapter struct {
	path              string
	schemaAware       bool
	bufferSize        int
	keepaliveInterval time.Duration
	logger            *slog.Logger
	schemaStore       schema.Store

	mu      sync.RWMutex
	clients map[*client]struct{}
	// subIndex maps subscription channels back to their subscription
	// so broadcast can efficiently iterate all active subscriptions.
	subs map[*subscription]*client
}

// New creates a new GraphQL subscriptions adapter.
func New(path string, schemaAware bool, bufferSize int, keepaliveInterval time.Duration, schemaStore schema.Store, logger *slog.Logger) *Adapter {
	if path == "" {
		path = "/graphql"
	}
	if bufferSize <= 0 {
		bufferSize = 256
	}
	if keepaliveInterval <= 0 {
		keepaliveInterval = 15 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		path:              path,
		schemaAware:       schemaAware,
		bufferSize:        bufferSize,
		keepaliveInterval: keepaliveInterval,
		logger:            logger.With("component", "graphql"),
		schemaStore:       schemaStore,
		clients:           make(map[*client]struct{}),
		subs:              make(map[*subscription]*client),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "graphql"
}

// Start reads events from the channel and broadcasts to matching subscriptions.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("graphql adapter started", "path", a.path)

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("graphql adapter stopping", "reason", "context cancelled")
			a.closeAllSubscriptions()
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				a.logger.Info("graphql adapter stopping", "reason", "channel closed")
				a.closeAllSubscriptions()
				return nil
			}
			a.broadcast(ev)
		}
	}
}

// MountHTTP registers the adapter's HTTP routes on the given router.
func (a *Adapter) MountHTTP(r chi.Router) {
	r.Get(a.path, a.ServeHTTP)
	if a.schemaAware {
		r.Get(a.path+"/schema", a.SchemaHandler)
	}
}

// broadcast sends an event to all subscriptions whose filter matches.
func (a *Adapter) broadcast(ev event.Event) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for sub := range a.subs {
		if !sub.filter.Matches(ev) {
			continue
		}
		select {
		case sub.ch <- ev:
			metrics.EventsDelivered.WithLabelValues("graphql").Inc()
		default:
			metrics.EventsDropped.WithLabelValues("graphql").Inc()
			a.logger.Warn("dropping event for slow graphql subscriber",
				"event_id", ev.ID,
				"channel", ev.Channel,
				"subscription_id", sub.id,
			)
		}
	}
}

// addSubscription registers a subscription for broadcast.
func (a *Adapter) addSubscription(c *client, sub *subscription) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subs[sub] = c
	a.clients[c] = struct{}{}
	a.logger.Debug("subscription added",
		"subscription_id", sub.id,
		"channel_filter", sub.filter.Channel,
		"total_subs", len(a.subs),
	)
}

// removeSubscription removes a single subscription.
func (a *Adapter) removeSubscription(_ *client, sub *subscription) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.subs, sub)
	close(sub.ch)
	a.logger.Debug("subscription removed",
		"subscription_id", sub.id,
		"total_subs", len(a.subs),
	)
}

// removeClient removes all subscriptions for a client.
func (a *Adapter) removeClient(c *client) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for sub, owner := range a.subs {
		if owner == c {
			delete(a.subs, sub)
			close(sub.ch)
			metrics.GraphQLSubscriptionsActive.Dec()
		}
	}
	delete(a.clients, c)
}

// closeAllSubscriptions closes all subscription channels on shutdown.
func (a *Adapter) closeAllSubscriptions() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for sub := range a.subs {
		close(sub.ch)
		metrics.GraphQLSubscriptionsActive.Dec()
	}
	a.subs = make(map[*subscription]*client)
	a.clients = make(map[*client]struct{})
}
