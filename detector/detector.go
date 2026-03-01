package detector

import (
	"context"

	"github.com/go-chi/chi/v5"

	"github.com/florinutz/pgcdc/event"
)

type Detector interface {
	Start(ctx context.Context, events chan<- event.Event) error
	Name() string
}

// HTTPMountable is optionally implemented by detectors that serve HTTP routes
// (e.g., the webhook gateway detector). When a detector implements this
// interface, the pipeline mounts its routes on the shared HTTP server.
type HTTPMountable interface {
	MountHTTP(r chi.Router)
}
