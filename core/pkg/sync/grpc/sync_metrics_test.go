package grpc

import (
	"context"
	"io"
	"testing"
	"time"

	"buf.build/gen/go/open-feature/flagd/grpc/go/flagd/sync/v1/syncv1grpc"
	v1 "buf.build/gen/go/open-feature/flagd/protocolbuffers/go/flagd/sync/v1"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/sync"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	msdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type metricTestStream struct {
	syncv1grpc.FlagSyncService_SyncFlagsClient
	receive func() (*v1.SyncFlagsResponse, error)
}

func (s metricTestStream) Recv() (*v1.SyncFlagsResponse, error) { return s.receive() }

func collectSyncGauges(t *testing.T, reader *msdk.ManualReader) []metricdata.DataPoint[float64] {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	var points []metricdata.DataPoint[float64]
	for _, scope := range data.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name == syncDisconnectedSeconds {
				points = append(points, instrument.Data.(metricdata.Gauge[float64]).DataPoints...)
			}
		}
	}
	return points
}

func TestSyncDisconnectionGaugeTracksRecoveryAndSubsequentLoss(t *testing.T) {
	reader := msdk.NewManualReader()
	provider := msdk.NewMeterProvider(msdk.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	g := &Sync{URI: "sync.example:8015", Logger: logger.NewLogger(nil, false)}
	start := time.Now()
	g.disconnectedSince.Store(&start)
	now := start.Add(10 * time.Minute)
	unregister := g.registerSyncState(provider.Meter("test"), func() time.Time { return now })
	t.Cleanup(unregister)
	read := func() float64 {
		points := collectSyncGauges(t, reader)
		require.Len(t, points, 1)
		return points[0].Value
	}
	require.Equal(t, 600.0, read())

	// Failed handshakes/retries must not reset the continuous-disconnection clock.
	g.markDisconnected()
	require.Error(t, g.handleFlagSync(eofFlagSyncClient{}, nil))
	require.Equal(t, 600.0, read())

	// Opening a stream does not clear it. Receiving and handing off flags does.
	calls := 0
	stream := metricTestStream{receive: func() (*v1.SyncFlagsResponse, error) {
		calls++
		if calls == 1 {
			require.Equal(t, 600.0, read())
			return &v1.SyncFlagsResponse{FlagConfiguration: "{}"}, nil
		}
		require.Zero(t, read())
		now = now.Add(time.Hour)
		require.Zero(t, read(), "an idle connected stream is not stale")
		return nil, io.EOF
	}}
	updates := make(chan sync.DataSync, 1)
	require.Error(t, g.handleFlagSync(stream, updates))
	require.Equal(t, "{}", (<-updates).FlagData)
	newLoss := g.disconnectedSince.Load()
	require.NotNil(t, newLoss)
	now = newLoss.Add(4 * time.Second)
	require.Equal(t, 4.0, read())
	g.markDisconnected()
	now = newLoss.Add(11 * time.Minute)
	require.Equal(t, 660.0, read(), "retry attempts must not erase a prolonged outage")
}

func TestSyncStateMetricsKeepSourcesSeparateAndUnregister(t *testing.T) {
	reader := msdk.NewManualReader()
	provider := msdk.NewMeterProvider(msdk.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	first := &Sync{URI: "first.example:8015"}
	second := &Sync{URI: "second.example:8015"}
	start := time.Now()
	first.disconnectedSince.Store(&start)
	now := func() time.Time { return start.Add(time.Minute) }
	stopFirst := first.registerSyncState(provider.Meter("test"), now)
	stopSecond := second.registerSyncState(provider.Meter("test"), now)
	points := collectSyncGauges(t, reader)
	require.Len(t, points, 2)
	for _, point := range points {
		replica, ok := point.Attributes.Value(attribute.Key("replica"))
		require.True(t, ok)
		require.NotEmpty(t, replica.AsString())
		source, ok := point.Attributes.Value(attribute.Key("sync_source"))
		require.True(t, ok)
		if source.AsString() == first.URI {
			require.Equal(t, 60.0, point.Value)
		} else {
			require.Equal(t, second.URI, source.AsString())
			require.Zero(t, point.Value)
		}
	}
	stopFirst()
	require.Len(t, collectSyncGauges(t, reader), 1)
	stopSecond()
	require.Empty(t, collectSyncGauges(t, reader))
}
