package kafka

import (
	"time"
)

type ConsumerGroupState struct {
	Config                    *ConsumerGroupConfig       `json:"config"`
	Status                    ConsumerGroupStatus        `json:"status"`
	ConsumerGroupHandlerState *ConsumerGroupHandlerState `json:"consumerGroupHandlerState"`
}
type ConsumerGroupStatus string

const (
	ConsumerGroupCreated      ConsumerGroupStatus = "CREATED"
	ConsumerGroupSubscribed   ConsumerGroupStatus = "SUBSCRIBED"
	ConsumerGroupUnsubscribed ConsumerGroupStatus = "UNSUBSCRIBED"
)

type ConsumerGroupHandlerState struct {
	GroupId             string                                     `json:"groupId"`
	Status              ConsumerGroupHandlerStatus                 `json:"status"`
	CreatedDate         time.Time                                  `json:"createdDate,omitempty"`
	ClosedDate          time.Time                                  `json:"closedDate,omitempty"`
	ConsumerTopicStates map[string]*ConsumerGroupHandlerTopicState `json:"consumerTopicStates"`
}

type ConsumerGroupHandlerStatus string

const (
	ConsumerGroupHandlerCreated ConsumerGroupHandlerStatus = "CREATED"
	ConsumerGroupHandlerStarted ConsumerGroupHandlerStatus = "STARTED"
	ConsumerGroupHandlerClosed  ConsumerGroupHandlerStatus = "CLOSED"
)

type ConsumerGroupHandlerTopicState struct {
	Topic                string                          `json:"topic"`
	Partition            int32                           `json:"partition"`
	Status               ConsumerGroupHandlerTopicStatus `json:"status"`
	CreatedDate          time.Time                       `json:"createdDate,omitempty"`
	ListeningDate        time.Time                       `json:"listeningDate,omitempty"`
	ClosedDate           time.Time                       `json:"closedDate,omitempty"`
	LatestConsumedOffset int64                           `json:"latestConsumedOffset,omitempty"`
	LatestConsumedDate   time.Time                       `json:"latestConsumedDate,omitempty"`
}

type ConsumerGroupHandlerTopicStatus string

const (
	ConsumerGroupHandlerTopicCreated    ConsumerGroupHandlerTopicStatus = "CREATED"
	ConsumerGroupHandlerTopicListening  ConsumerGroupHandlerTopicStatus = "LISTENING"
	ConsumerGroupHandlerTopicStarted    ConsumerGroupHandlerTopicStatus = "STARTED"
	ConsumerGroupHandlerTopicClosed     ConsumerGroupHandlerTopicStatus = "CLOSED"
	ConsumerGroupHandlerTopicNewMessage ConsumerGroupHandlerTopicStatus = "NEW_MESSAGE"
)
