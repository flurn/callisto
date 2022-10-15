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

type RetryMessage struct {
	RetryCounter          int       `json:"retry_counter"`
	ExpectedExecutionTime time.Time `json:"expected_execution_time"`
	*k.Message
}
