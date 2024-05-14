package tracers

import (
	"context"
	"presentation-advert-consumer/application/tracers"
)

type otelTracer struct{}

func NewOtelTracer() tracers.Tracer {
	return &otelTracer{}
}

func (tc *otelTracer) Trace(ctx context.Context, structName, funcName string) (context.Context, tracers.DeferFunc) {
	//span := trace.SpanFromContext(ctx)
	//if span == nil || !span.SpanContext().IsValid() {
	//	return ctx, func() {
	//		// do nothing
	//	}
	//}
	//tracer := span.TracerProvider().Tracer(structName)
	//childCtx, childSpan := tracer.Start(ctx, funcName)
	//return childCtx, func() {
	//	childSpan.End()
	//}
	return ctx, func() {}
}
