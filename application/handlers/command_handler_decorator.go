package handlers

import (
	"context"
	"presentation-advert-consumer/application/tracers"
	"reflect"
)

type CommandHandlerInterface[T any, R any] interface {
	Handle(ctx context.Context, value T) R
}

type CommandHandlerDecorator[T any, R any] interface {
	Handle(ctx context.Context, value T) R
}

type commandHandlerDecorator[T any, R any] struct {
	tracers []tracers.Tracer
	handler CommandHandlerInterface[T, R]
}

func NewCommandHandlerDecorator[T any, R any](handler CommandHandlerInterface[T, R], tracers []tracers.Tracer) CommandHandlerDecorator[T, R] {
	return &commandHandlerDecorator[T, R]{
		tracers: tracers,
		handler: handler,
	}
}

func (decorator *commandHandlerDecorator[T, R]) Handle(ctx context.Context, value T) R {
	deferFunctions := make([]tracers.DeferFunc, 0, len(decorator.tracers))
	for _, decoratorTracer := range decorator.tracers {
		var deferFunction tracers.DeferFunc
		ctx, deferFunction = decoratorTracer.Trace(ctx, reflect.TypeOf(decorator.handler).String(), "Handle")
		deferFunctions = append(deferFunctions, deferFunction)
	}
	result := decorator.handler.Handle(ctx, value)
	for _, function := range deferFunctions {
		function()
	}
	return result
}
