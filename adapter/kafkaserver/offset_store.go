package kafkaserver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/checkpoint"
)

// offsetStore persists consumer group offsets using the checkpoint.Store.
// Keys are formatted as: kafka:{group}:{topic}:{partition}
type offsetStore struct {
	store  checkpoint.Store
	logger *slog.Logger
}

func newOffsetStore(store checkpoint.Store, logger *slog.Logger) *offsetStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &offsetStore{
		store:  store,
		logger: logger.With("component", "kafkaserver-offsets"),
	}
}

// slotName builds the checkpoint key for a given group/topic/partition.
func slotName(group, topic string, partition int32) string {
	return fmt.Sprintf("kafka:%s:%s:%d", group, topic, partition)
}

// commitOffset persists the committed offset for a partition.
func (s *offsetStore) commitOffset(ctx context.Context, group, topic string, partition int32, offset int64) error {
	key := slotName(group, topic, partition)
	if err := s.store.Save(ctx, key, uint64(offset)); err != nil {
		return fmt.Errorf("commit offset: %w", err)
	}
	s.logger.Debug("offset committed",
		"group", group,
		"topic", topic,
		"partition", partition,
		"offset", offset,
	)
	return nil
}

// fetchOffset loads the last committed offset for a partition.
// Returns -1 if no offset has been committed.
func (s *offsetStore) fetchOffset(ctx context.Context, group, topic string, partition int32) (int64, error) {
	key := slotName(group, topic, partition)
	val, err := s.store.Load(ctx, key)
	if err != nil {
		return -1, fmt.Errorf("fetch offset: %w", err)
	}
	if val == 0 {
		return -1, nil // no committed offset
	}
	return int64(val), nil
}
