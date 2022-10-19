package consumer

import (
	"context"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/flurn/callisto/logger"
)

type kafkaReader struct {
	consumer *k.Consumer
	context  context.Context
	msg      chan *k.Message
}

func (r *kafkaReader) Serve() {
	for {
		select {
		case <-r.context.Done():
			logger.Errorf("kafka message reader routine exiting")
			return
		default:
			message, err := r.consumer.ReadMessage(-1)
			if err != nil {
				//logger.Errorf("kafka consumer failure %v", err)
				continue
			}

			if message == nil {
				continue
			}
			r.msg <- message
		}
	}
}

func (r *kafkaReader) Stop() {
}
