package consumer

import (
	"context"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/flurn/callisto/logger"
)

type kafkaCommitter struct {
	consumer *k.Consumer
	context  context.Context
	ack      chan *k.Message
}

func (c *kafkaCommitter) Serve() {
	for {
		select {
		case <-c.context.Done():
			logger.Error("kafka commit message routine exiting")
			return
		case message := <-c.ack:
			_, err := c.consumer.CommitMessage(message)
			if err != nil {
				logger.Errorf("failed to commit offset %v", err)
				logger.Debugf("failed to commit offset for message %v with error %v", message, err)
			}
		}
	}
}

func (c *kafkaCommitter) Stop() {
}
