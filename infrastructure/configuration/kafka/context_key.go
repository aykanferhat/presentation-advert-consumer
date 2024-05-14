package kafka

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const (
	RetryTopicCountKey ContextKey = "X-RetryCount"
	ErrorTopicCountKey ContextKey = "X-ErrorCount"
	TargetTopicKey     ContextKey = "X-TargetTopic"
	ErrorMessageKey    ContextKey = "X-ErrorMessage"
	CorrelationIdKey   ContextKey = "X-CorrelationId"
)
