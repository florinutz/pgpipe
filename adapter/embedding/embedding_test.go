package embedding_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/embedding"
)

func TestEmbeddingAdapter_Name(t *testing.T) {
	a := embedding.New(
		"http://localhost:11434/v1/embeddings", "test-key", "",
		[]string{"title", "body"}, "",
		"postgres://test", "", 0, 0, 0, 0, 0,
		false,
		0, 0, 0, 0,
		nil,
	)
	if got := a.Name(); got != "embedding" {
		t.Errorf("Name() = %q, want %q", got, "embedding")
	}
}
