package config

import (
	"context"

	"github.com/flurn/callisto/types"
)

const (
	transactionalScope = "transactional"
	bulkScope          = "bulk"
	internalScope      = "internal"
)

type KafkaConfig struct {
	brokerList                string
	keepAliveEnabled          bool
	consumeOffset             string
	consumerGroup             string
	maxConnections            int
	transactionalPublishTopic string
	workers                   int
	lingerMs                  int
	messageBatchSize          int
	messageTimeoutMs          int
	username                  string // confluent api key
	password                  string // confluent api secret
}

type RetryConfig struct {
	Type          types.RetryType
	MaxRetries    int
	ErrorCallback func(msg []byte, err error)
}

type KafkaTopicConfig struct {
	TopicName           string
	Retry               *RetryConfig
	ConsumerMessagePSec int
}

func (kc *KafkaConfig) Username() string {
	return kc.username
}

func (kc *KafkaConfig) Password() string {
	return kc.password
}

func (kc KafkaConfig) BrokerList() string {
	return kc.brokerList
}

func (kc KafkaConfig) KeepAlive() bool {
	return kc.keepAliveEnabled
}

func (kc KafkaConfig) MaxConnections() int {
	return kc.maxConnections
}

func (kc KafkaConfig) TransactionalPublishTopic() string {
	return kc.transactionalPublishTopic
}

func (kc KafkaConfig) ConsumeOffset() string {
	return kc.consumeOffset
}

func (kc KafkaConfig) ConsumerGroup() string {
	return kc.consumerGroup
}

func (kc KafkaConfig) Workers() int {
	return kc.workers
}

func (kc KafkaConfig) LingerMs() int {
	return kc.lingerMs
}

func (kc KafkaConfig) MessageBatchSize() int {
	return kc.messageBatchSize
}

func (kc KafkaConfig) MessageTimeoutMs() int {
	return kc.messageTimeoutMs
}

func (kc KafkaConfig) KafkaPublishTopic(ctx context.Context) string {
	publishScope := ctx.Value(types.PublishHeader)

	switch publishScope {
	case transactionalScope:
		return kc.TransactionalPublishTopic()
	default:
		return kc.TransactionalPublishTopic()
	}
}

func newKafkaConfig() KafkaConfig {
	kc := KafkaConfig{
		username:                  mustGetString("KAFKA_CLUSTER_USERNAME"),
		password:                  mustGetString("KAFKA_CLUSTER_PASSWORD"),
		brokerList:                mustGetString("KAFKA_BROKER_LIST"),
		maxConnections:            mustGetInt("KAFKA_MAX_CONNECTIONS"),
		transactionalPublishTopic: mustGetString("KAFKA_TRANSACTIONAL_PUBLISH_TOPIC"),
		consumeOffset:             mustGetString("KAFKA_CONSUME_OFFSET"),
		consumerGroup:             mustGetString("KAFKA_CONSUMER_GROUP"),
		workers:                   mustGetInt("KAFKA_WORKER_COUNT"),
		lingerMs:                  mustGetInt("KAFKA_LINGER_MS"),
		messageTimeoutMs:          mustGetInt("KAFKA_MESSAGE_TIMEOUT_MS"),
		messageBatchSize:          mustGetInt("KAFKA_PUBLISH_BATCH_SIZE"),
	}

	return kc
}

func consumerASWorkerConfig() KafkaConfig {
	kc := KafkaConfig{
		username:         mustGetString("KAFKA_CLUSTER_USERNAME"),
		password:         mustGetString("KAFKA_CLUSTER_PASSWORD"),
		brokerList:       mustGetString("CONS_KAFKA_BROKER_LIST"),
		maxConnections:   mustGetInt("CONS_KAFKA_MAX_CONNECTIONS"),
		consumeOffset:    mustGetString("CONS_KAFKA_CONSUME_OFFSET"),
		consumerGroup:    mustGetString("CONS_KAFKA_CONSUMER_GROUP"),
		workers:          mustGetInt("CONS_KAFKA_WORKER_COUNT"),
		lingerMs:         mustGetInt("CONS_KAFKA_LINGER_MS"),
		messageTimeoutMs: mustGetInt("CONS_KAFKA_MESSAGE_TIMEOUT_MS"),
		messageBatchSize: mustGetInt("CONS_KAFKA_PUBLISH_BATCH_SIZE"),
	}

	return kc
}
