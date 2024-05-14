package kafka

import (
	"context"
)

type Consumer interface {
	Consume(ctx context.Context, message *ConsumerMessage) error
}
