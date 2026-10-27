package grpc

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const syncDisconnectedSeconds = "feature_flag.flagd.sync.client.disconnected_seconds"

// A timestamp is retained across failed reconnect attempts and cleared only
// after a new flag payload is handed to the runtime. A connected, idle stream
// therefore reports zero regardless of how long ago the flags last changed.
func (g *Sync) markDisconnected() {
	now := time.Now()
	g.disconnectedSince.CompareAndSwap(nil, &now)
}

func (g *Sync) disconnectedSeconds(now time.Time) float64 {
	if since := g.disconnectedSince.Load(); since != nil {
		return now.Sub(*since).Seconds()
	}
	return 0
}

func (g *Sync) observeSyncState() func() {
	return g.registerSyncState(otel.Meter("github.com/open-feature/flagd/core/pkg/sync/grpc"), time.Now)
}

func (g *Sync) registerSyncState(meter metric.Meter, now func() time.Time) func() {
	replica, _ := os.Hostname()
	attrs := metric.WithAttributes(
		attribute.String("replica", replica),
		attribute.String("sync_source", g.URI),
	)
	gauge, err := meter.Float64ObservableGauge(syncDisconnectedSeconds,
		metric.WithUnit("s"),
		metric.WithDescription("Continuous time without an up-to-date upstream sync stream; zero while connected."))
	if err != nil {
		otel.Handle(err)
		return func() {}
	}
	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveFloat64(gauge, g.disconnectedSeconds(now()), attrs)
		return nil
	}, gauge)
	if err != nil {
		otel.Handle(err)
		return func() {}
	}
	return func() {
		if err := registration.Unregister(); err != nil {
			otel.Handle(err)
		}
	}
}
