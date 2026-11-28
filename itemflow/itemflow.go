// Package itemflow puts a call made for one item in that item's flow. An
// item's flow id is a W3C trace id, so a call carries it as the trace id of its
// traceparent, under a span of its own, and names the item's key in its
// baggage. This is W3C Trace Context's propagation, as OpenTelemetry's
// instrumented clients carry a trace across a service's calls: the trace rides
// in the call's context, and the client's transport writes it as headers. A
// worker's service clients write it through goclikit.TraceForwardingTransport,
// so a destination or a dispatch service that reads trace context places each
// call in the item's trace.
package itemflow

import (
	"context"
	"crypto/rand"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

// KeyBaggage is the baggage member that names the item a call is made for.
const KeyBaggage = "ingestion.item.key"

// Join is ctx in the flow of the item under key: a new sampled span in the
// trace that flow names, as ctx's remote parent, and baggage naming key beside
// ctx's other members. Each call made in it sends
// traceparent 00-<flow>-<that span>-01. A flow that is no trace id leaves
// ctx's span as it is, and the key still rides in its baggage.
func Join(ctx context.Context, flow, key string) context.Context {
	if traceID, err := trace.TraceIDFromHex(flow); err == nil {
		ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID, SpanID: newSpanID(), TraceFlags: trace.FlagsSampled, Remote: true,
		}))
	}
	member, err := baggage.NewMemberRaw(KeyBaggage, key)
	if err != nil {
		return ctx
	}
	b, err := baggage.FromContext(ctx).SetMember(member)
	if err != nil {
		return ctx
	}
	return baggage.ContextWithBaggage(ctx, b)
}

// newSpanID is 64 random bits, never all zeros.
func newSpanID() trace.SpanID {
	for {
		var id trace.SpanID
		_, _ = rand.Read(id[:])
		if id.IsValid() {
			return id
		}
	}
}
