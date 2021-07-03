package kafka

import (
	"context"
	"errors"
	"fmt"

	"callisto/config"
	"callisto/logger"
	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/getsentry/raven-go"
)

type kafka struct {
	producer *k.Producer
}

func (kq *kafka) Push(ctx context.Context, msg []byte, topic string) error {

	message := &k.Message{
		TopicPartition: k.TopicPartition{Topic: &topic, Partition: k.PartitionAny},
		Value:          msg,
	}

	deliveryChan := make(chan k.Event, 1)
	err := kq.producer.Produce(message, deliveryChan)
	if err != nil {
		logger.Errorf("Kafka queue failed to publish message to kafka: %v", err)
		return err
	}

	ev := <-deliveryChan
	switch e := ev.(type) {
	case *k.Message:
		if e.TopicPartition.Error != nil {
			logKafkaError(e.TopicPartition.Error.Error())
			return errors.New(e.TopicPartition.Error.Error())
		}
	case k.Error:
		logKafkaError(e.Error())
		return errors.New(e.Error())
	default:
		logKafkaError(fmt.Sprintf("unhandled kafka event: %v", e))
		return errors.New("unhandled kafka event")
	}

	return nil
}

func logKafkaError(err string) {
	logger.Errorf("Kafka Queue Delivery failed: %v\n", err)
	raven.CaptureMessage("KafkaPush", map[string]string{
		"KafkaPush": err,
	})
}

func NewKafkaClient(kafkaConfig config.KafkaConfig) *kafka {
	producer, err := k.NewProducer(confluentKafkaConfig(kafkaConfig))
	if err != nil {
		panic(err)
	}

	return &kafka{producer: producer}
}

func confluentKafkaConfig(kafkaConfig config.KafkaConfig) *k.ConfigMap {
	return &k.ConfigMap{
		"bootstrap.servers":       kafkaConfig.BrokerList(),
		"socket.keepalive.enable": kafkaConfig.KeepAlive(),
		"client.id":               "app_name",
		"api.version.request":     "true",
		"default.topic.config": k.ConfigMap{
			"partitioner": "consistent_random",
		},
		"linger.ms":          kafkaConfig.LingerMs(),
		"retry.backoff.ms":   kafkaConfig.RetryBackoffMs(),
		"batch.num.messages": kafkaConfig.MessageBatchSize(),
		"message.timeout.ms": kafkaConfig.MessageTimeoutMs(),
	}
}
