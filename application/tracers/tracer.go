package tracers

import (
	"context"
)

type Tracer interface {
	Trace(ctx context.Context, structName, funcName string) (context.Context, DeferFunc)
}

type DeferFunc func()
