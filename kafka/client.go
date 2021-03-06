package kafka

import (
	"github.com/flurn/callisto/config"
	"context"
)

type Client interface {
	Push(ctx context.Context, msg []byte, topic string) error
}

func GetClient() Client {
	return NewKafkaClient(config.Kafka())
}

func CreateTopics(topicNames []string, kafkaConfig config.KafkaConfig) {
	for _, topicName := range topicNames {
		CreateTopic(topicName, kafkaConfig)
	}
}
