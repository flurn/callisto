package consumer

import (
	"encoding/json"
	"fmt"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/helper"
	"github.com/flurn/callisto/logger"
	"github.com/flurn/callisto/retry"
	"github.com/flurn/callisto/types"
)

func (c *Consumer) Retry(fn func(msg []byte) error, msg *k.Message, rc *config.RetryConfig) error {
	switch rc.Type {
	case types.RetryTypeExponential:
		return c.ExponentialBackOff(fn, msg, rc)
	case types.RetryTypeCustom:
		return c.ExponentialBackOff(fn, msg, rc)
	case types.RetryTypeFifo:
		return c.fifoBackOff(fn, msg, rc)
	default:
		logger.Errorf("Unknown retry type: %s", rc.Type)
		return fmt.Errorf("unknown retry type: %s", rc.Type)
	}
}

func (c *Consumer) fifoBackOff(fn func(msg []byte) error, msg *k.Message, rc *config.RetryConfig) error {
	backOff := retry.NewExponentialBackOff(initialTimeout, maxTimeout, exponentFactor)
	for {
		err := fn(msg.Value)

		// break on max retries
		if backOff.GetRetryCounter() >= rc.MaxRetries {
			logger.Errorf("Max retries reached for message: %s", string(msg.Value))

			// move to DLQ
			err := c.producer(c.topicName+types.DLQ_Postfix, msg.Value)
			if err != nil {
				logger.Errorf("Failed to move message to DLQ, err: %v", err)
			}
			return err
		}

		if err != nil {
			time.Sleep(backOff.Next())
		} else {
			return nil
		}
	}
}

func (c *Consumer) ExponentialBackOff(fn func(msg []byte) error, msg *k.Message, rc *config.RetryConfig) error {
	if rc.MaxRetries == 0 {
		// move to DLQ
		rc.RetryFailedCallback(msg.Value, fmt.Errorf("max retries reached"))
		err := c.producer(c.topicName+types.DLQ_Postfix, msg.Value)
		return err
	}

	retryTopicName := helper.GetNextRetryTopicName(c.topicName, 1)
	// get backOff time
	backOffTime := helper.GetBackOffTimeInMilliSeconds(0, rc.Type)

	// create retry message wrapper
	retryMessage := &types.RetryMessage{
		RetryCounter:          1,
		Message:               msg,
		ExpectedExecutionTime: time.Now().Add(time.Duration(backOffTime * int(time.Microsecond))),
	}

	retryMessageBytes, err := json.Marshal(retryMessage)
	if err != nil {
		return err
	}

	// produce message to next retry topic
	err = c.producer(retryTopicName, retryMessageBytes)
	if err != nil {
		logger.Errorf("Failed to move message to retry topic %s, err: %v", retryTopicName, err)
		rc.RetryFailedCallback(msg.Value, err)
		return err
	}

	return nil
}
