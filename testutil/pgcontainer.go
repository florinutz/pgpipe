package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PostgresContainer holds a running PostgreSQL container and its connection string.
type PostgresContainer struct {
	Container testcontainers.Container
	ConnStr   string
}

// StartPostgres starts a PostgreSQL container suitable for integration tests.
// The container is automatically terminated when the test ends.
func StartPostgres(t *testing.T) *PostgresContainer {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "pgpipe",
			"POSTGRES_PASSWORD": "pgpipe",
			"POSTGRES_DB":       "pgpipe_test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get mapped port: %v", err)
	}

	connStr := fmt.Sprintf("postgres://pgpipe:pgpipe@%s:%s/pgpipe_test?sslmode=disable", host, port.Port())

	// Verify connectivity.
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	_ = conn.Close(ctx)

	return &PostgresContainer{
		Container: container,
		ConnStr:   connStr,
	}
}
