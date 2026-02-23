package detector

import (
	"context"

	"github.com/florinutz/pgcdc/event"
)

type Detector interface {
	Start(ctx context.Context, events chan<- event.Event) error
	Name() string
}
