package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init sets up the global tracer provider, exporting spans to the OTLP
// endpoint (Jaeger). It returns a shutdown function to flush spans on exit.
func Init(ctx context.Context, serviceName, otlpEndpoint string) (func(context.Context) error, error) {
	// The exporter ships finished spans to Jaeger over OTLP gRPC.
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(), // no TLS for local dev
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	// The resource describes WHO is producing these spans — the service name
	// is what appears in Jaeger's service dropdown.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	// The tracer provider ties it together: batch finished spans and export them.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Register globally so otel.Tracer(...) anywhere uses this provider.
	otel.SetTracerProvider(tp)

	// The propagator controls how trace context is injected/extracted across
	// boundaries (HTTP headers, Kafka message headers). W3C Trace Context is
	// the standard — we'll use this in the Kafka step.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}
