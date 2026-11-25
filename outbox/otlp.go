package outbox

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

// scope is the instrumentation scope every record names.
const scope = "github.com/datapointchris/ingestion/outbox"

// resourceOf is what every record ingestion ships carries: the service, its
// release, the process, and the node under it where NODE_NAME names one.
func resourceOf(version string) *resource.Resource {
	instance, _ := os.Hostname()
	attributes := []attribute.KeyValue{
		attribute.String("service.name", "ingestion"),
		attribute.String("service.version", version),
		attribute.String("service.instance.id", instance),
	}
	if node := os.Getenv("NODE_NAME"); node != "" {
		attributes = append(attributes, attribute.String("k8s.node.name", node))
	}
	return resource.NewSchemaless(attributes...)
}

// otlpShipper sends each batch to endpoint as one OTLP/HTTP export, whose
// answer is the batch's acknowledgement. The exporter retries nothing itself,
// so the relay's own wait paces a retry.
func otlpShipper(ctx context.Context, endpoint, version string) (func(context.Context, []record) error, error) {
	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(endpoint),
		otlploghttp.WithCompression(otlploghttp.GzipCompression),
		otlploghttp.WithTimeout(exportTimeout),
		otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}),
	)
	if err != nil {
		return nil, fmt.Errorf("the OTLP exporter for %s: %w", endpoint, err)
	}
	kept := &keptRecords{}
	logger := sdklog.NewLoggerProvider(sdklog.WithResource(resourceOf(version)), sdklog.WithProcessor(kept)).
		Logger(scope, otellog.WithInstrumentationVersion(version))
	return func(ctx context.Context, records []record) error {
		kept.records = kept.records[:0]
		observed := time.Now()
		sent := observed.UTC().Format(time.RFC3339Nano)
		for _, r := range records {
			var lr otellog.Record
			lr.SetTimestamp(r.at)
			lr.SetObservedTimestamp(observed)
			lr.SetSeverity(otellog.SeverityInfo)
			lr.SetBody(attribute.StringValue(string(r.body)))
			lr.AddAttributes(
				attribute.String("fleet.kind", r.fleetKind),
				attribute.String("ingestion.record.kind", r.kind),
				attribute.String("ingestion.record.id", r.id),
				attribute.String("ingestion.record.version", r.version),
				attribute.String("ingestion.record.sent_ts", sent),
			)
			logger.Emit(ctx, lr)
		}
		return exporter.Export(ctx, kept.records)
	}, nil
}

// keptRecords is a log processor that keeps what is emitted. The SDK stamps
// each record with the resource and scope, and the batch then goes out as one
// export call rather than through the SDK's own batching and retries.
type keptRecords struct{ records []sdklog.Record }

func (k *keptRecords) OnEmit(_ context.Context, r *sdklog.Record) error {
	k.records = append(k.records, r.Clone())
	return nil
}

func (*keptRecords) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (*keptRecords) Shutdown(context.Context) error                         { return nil }
func (*keptRecords) ForceFlush(context.Context) error                       { return nil }
