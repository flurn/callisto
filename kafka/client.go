package kafka

import (
	"context"

	"github.com/flurn/callisto/config"
)

type Client interface {
	Push(ctx context.Context, msg []byte, topic string) error
}

func GetClient() Client {
	return NewKafkaClient(config.Kafka())
}

func CreateTopics(topicConfigs []*config.KafkaTopicConfig, kafkaConfig config.KafkaConfig) {
	for _, topicConfig := range topicConfigs {
		CreateTopic(topicConfig, kafkaConfig)
	}
}
