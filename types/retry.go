package types

import (
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
)

type RetryType string

const (
	RetryTypeExponential RetryType = "exponential"
	RetryTypeFixed       RetryType = "fixed"
	RetryTypeFifo        RetryType = "fifo"
	RetryTypeCustom      RetryType = "custom"
)

type RetryConfig struct {
	Type                RetryType
	MaxRetries          int
	RetryFailedCallback func(msg []byte, err error)
}

type RetryMessage struct {
	MessageCounter        int
	ExpectedExecutionTime time.Time
	*k.Message
}
