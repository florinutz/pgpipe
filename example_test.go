package pgcdc_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/florinutz/pgcdc/presets"
	"github.com/florinutz/pgcdc/transform"
)

// Example_walToStdout demonstrates streaming WAL changes to stdout.
// The pipeline reads logical replication events from the "my_pub" publication
// and prints each event as a JSON line.
func Example_walToStdout() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	p := presets.WALToStdout(
		"postgres://user:pass@localhost:5432/mydb",
		"my_pub",
		presets.WithSlotName("my_slot"),
		presets.WithBusBuffer(2048),
		presets.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	)

	if err := p.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

// Example_listenNotifyToWebhook demonstrates streaming LISTEN/NOTIFY
// events to a webhook endpoint. A transform drops the "password" column
// from every event before delivery.
func Example_listenNotifyToWebhook() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	p := presets.ListenNotifyToWebhook(
		"postgres://user:pass@localhost:5432/mydb",
		[]string{"orders", "payments"},
		"https://example.com/webhook",
		presets.WithTransforms(
			transform.DropColumns("password", "ssn"),
		),
	)

	if err := p.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
