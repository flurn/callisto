package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/helper"
	"github.com/flurn/callisto/logger"
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
		"security.protocol":       "SASL_SSL",
		"sasl.mechanisms":         "PLAIN",
		"api.version.request":     "true",
		"default.topic.config": k.ConfigMap{
			"partitioner": "consistent_random",
		},
		"sasl.username":        kafkaConfig.Username(),
		"sasl.password":        kafkaConfig.Password(),
		"linger.ms":            kafkaConfig.LingerMs(),
		"batch.num.messages":   kafkaConfig.MessageBatchSize(),
		"message.timeout.ms":   kafkaConfig.MessageTimeoutMs(),
		"max.poll.interval.ms": int((7 * time.Minute).Milliseconds()), // max poll for kafka retry consumer
	}
}

func checkKafkaTopicCreateError(results []k.TopicResult) {
	for _, result := range results {
		if result.Error.Code() != k.ErrNoError &&
			result.Error.Code() != k.ErrTopicAlreadyExists {
			logger.Errorf("Kafka topic creation failed for %s: %v", result.Topic, result.Error)
			os.Exit(1)
		}
	}
}

func CreateTopic(topicConfig *config.KafkaTopicConfig, kafkaConfig config.KafkaConfig) {
	adminClient, err := k.NewAdminClient(confluentKafkaConfig(kafkaConfig))
	if err != nil {
		logger.Errorf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer adminClient.Close()

	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDuration, err := time.ParseDuration("60s")
	if err != nil {
		panic("Panic: time.ParseDuration(60s)")
	}

	// create main and dead letter topic
	results, err := adminClient.CreateTopics(ctx,
		[]k.TopicSpecification{
			{
				Topic:             topicConfig.TopicName,
				NumPartitions:     1,
				ReplicationFactor: 3,
			},
			{
				Topic:             helper.GetDLQTopicName(topicConfig.TopicName),
				NumPartitions:     1,
				ReplicationFactor: 3,
			},
		},
		k.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		logger.Errorf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	checkKafkaTopicCreateError(results)

	// create retry topic
	if topicConfig.Retry != nil && topicConfig.Retry.MaxRetries > 0 {
		topicSpecification := []k.TopicSpecification{}
		for i := 1; i <= topicConfig.Retry.MaxRetries; i++ {
			retryTopicName := ""
			if len(topicConfig.Retry.RetryTopics) < i+1 {
				retryTopicName = helper.GetNextRetryTopicName(topicConfig.TopicName, i)
			} else {
				retryTopicName = topicConfig.Retry.RetryTopics[i]
			}

			// create all retry topics with 1 partition
			topicSpecification = append(topicSpecification, k.TopicSpecification{
				Topic:             retryTopicName,
				NumPartitions:     1,
				ReplicationFactor: 3,
			})
		}
		maxDuration := 5*time.Minute + 30*time.Second
		results, err = adminClient.CreateTopics(ctx, topicSpecification, k.SetAdminOperationTimeout(maxDuration))
		if err != nil {
			logger.Errorf("Problem during the topic creation: %v\n", err)
			os.Exit(1)
		}
		checkKafkaTopicCreateError(results)
	}
}
