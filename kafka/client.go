package kafka

import (
	"callisto/config"
	"context"
)

type Client interface {
	Push(ctx context.Context, msg []byte, topic string) error
}

func GetClient() Client {
	return NewKafkaClient(config.Kafka())
}
