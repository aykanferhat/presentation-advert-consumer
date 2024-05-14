package kafka

import (
	"context"
	"github.com/hashicorp/go-uuid"
)

type MessageHeader struct {
	Key   string
	Value interface{}
}

func AddHeaderToContext(ctx context.Context, key ContextKey, value any) context.Context {
	values := ctx.Value(key)
	if values == nil {
		return context.WithValue(ctx, key, value)
	}
	return nil
}

func AddHeadersToContext(ctx context.Context, headers map[ContextKey]any) context.Context {
	for key, value := range headers {
		ctx = AddHeaderToContext(ctx, key, value)
	}
	return ctx
}

func GetHeaderFromContext[T any](ctx context.Context, key ContextKey) T {
	values := ctx.Value(key)
	var result T
	if values == nil {
		return result
	}

	return values.(T)
}

func AddCorrelationIdToContextIfDoesNotExists(ctx context.Context) context.Context {
	correlationId := GetHeaderFromContext[string](ctx, CorrelationIdKey)
	if correlationId != "" {
		return ctx
	}
	correlationId, _ = uuid.GenerateUUID()
	return AddHeaderToContext(ctx, CorrelationIdKey, correlationId)
}
