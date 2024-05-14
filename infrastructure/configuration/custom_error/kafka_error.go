package custom_error

import (
	"fmt"
	"time"
)

type KafkaPartitionScalingErr struct {
	Detail  string    `json:"detail"`
	Instant time.Time `json:"instant"`
}

func (err KafkaPartitionScalingErr) Error() string {
	return err.Detail
}

func NewErrWithArgs(detail string, a ...any) error {
	return NewErr(fmt.Sprintf(detail, a...))
}

func NewErr(detail string) error {
	return &KafkaPartitionScalingErr{
		Detail:  detail,
		Instant: time.Now(),
	}
}
