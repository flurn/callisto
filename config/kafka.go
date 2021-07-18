package config

import (
	"callisto/types"
	"context"
)

const (
	transactionalScope = "transactional"
	bulkScope          = "bulk"
	internalScope      = "internal"
)

type KafkaConfig struct {
	brokerList                string
	keepAliveEnabled          bool
	backOff                   int
	bulkPublishTopic          string
	consumeOffset             string
	consumerGroup             string
	internalPublishTopic      string
	maxConnections            int
	topics                    []string
	transactionalPublishTopic string
	workers                   int
	lingerMs                  int
	retryBackOffMs            int
	messageBatchSize          int
	messageTimeoutMs          int
}

func (kc KafkaConfig) Topics() []string {
	return kc.topics
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

func (kc KafkaConfig) BulkPublishTopic() string {
	return kc.bulkPublishTopic
}

func (kc KafkaConfig) InternalPublishTopic() string {
	return kc.internalPublishTopic
}

func (kc KafkaConfig) TransactionalPublishTopic() string {
	return kc.transactionalPublishTopic
}

func (kc KafkaConfig) BackOff() int {
	return kc.backOff
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

func (kc KafkaConfig) RetryBackoffMs() int {
	return kc.retryBackOffMs
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
	case bulkScope:
		return kc.BulkPublishTopic()
	case internalScope:
		return kc.InternalPublishTopic()
	default:
		return kc.TransactionalPublishTopic()
	}
}

func newKafkaConfig() KafkaConfig {
	kc := KafkaConfig{
		brokerList:                mustGetString("KAFKA_BROKER_LIST"),
		bulkPublishTopic:          mustGetString("KAFKA_BULK_PUBLISH_TOPIC"),
		internalPublishTopic:      mustGetString("KAFKA_INTERNAL_PUBLISH_TOPIC"),
		maxConnections:            mustGetInt("KAFKA_MAX_CONNECTIONS"),
		transactionalPublishTopic: mustGetString("KAFKA_TRANSACTIONAL_PUBLISH_TOPIC"),
		backOff:                   mustGetInt("KAFKA_RETRY_IN_MILLISECONDS"),
		consumeOffset:             mustGetString("KAFKA_CONSUME_OFFSET"),
		consumerGroup:             mustGetString("KAFKA_CONSUMER_GROUP"),
		workers:                   mustGetInt("KAFKA_WORKER_COUNT"),
		lingerMs:                  mustGetInt("KAFKA_LINGER_MS"),
		retryBackOffMs:            mustGetInt("KAFKA_RETRY_BACKOFF_MS"),
		messageTimeoutMs:          mustGetInt("KAFKA_MESSAGE_TIMEOUT_MS"),
		messageBatchSize:          mustGetInt("KAFKA_PUBLISH_BATCH_SIZE"),
	}

	kc.topics = []string{kc.bulkPublishTopic, kc.internalPublishTopic, kc.transactionalPublishTopic}
	return kc
}

func consumerASWorkerConfig() KafkaConfig {
	kc := KafkaConfig{
		brokerList:                mustGetString("CONS_KAFKA_BROKER_LIST"),
		bulkPublishTopic:          mustGetString("CONS_KAFKA_BULK_PUBLISH_TOPIC"),
		internalPublishTopic:      mustGetString("CONS_KAFKA_INTERNAL_PUBLISH_TOPIC"),
		maxConnections:            mustGetInt("CONS_KAFKA_MAX_CONNECTIONS"),
		transactionalPublishTopic: mustGetString("CONS_KAFKA_TRANSACTIONAL_PUBLISH_TOPIC"),
		backOff:                   mustGetInt("CONS_KAFKA_RETRY_IN_MILLISECONDS"),
		consumeOffset:             mustGetString("CONS_KAFKA_CONSUME_OFFSET"),
		consumerGroup:             mustGetString("CONS_KAFKA_CONSUMER_GROUP"),
		workers:                   mustGetInt("CONS_KAFKA_WORKER_COUNT"),
		lingerMs:                  mustGetInt("CONS_KAFKA_LINGER_MS"),
		retryBackOffMs:            mustGetInt("CONS_KAFKA_RETRY_BACKOFF_MS"),
		messageTimeoutMs:          mustGetInt("CONS_KAFKA_MESSAGE_TIMEOUT_MS"),
		messageBatchSize:          mustGetInt("CONS_KAFKA_PUBLISH_BATCH_SIZE"),
	}

	kc.topics = []string{kc.bulkPublishTopic, kc.internalPublishTopic, kc.transactionalPublishTopic}
	return kc
}
