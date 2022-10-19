package consumer

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/thejerf/suture"

	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/helper"
	"github.com/flurn/callisto/kafka"
	"github.com/flurn/callisto/logger"
	"github.com/flurn/callisto/types"
)

const (
	logFormat    = "[Consumer]: %s, %+v"
	oldestOffset = "earliest"
	latestOffset = "latest"

	exponentFactor = 2
	initialTimeout = 1
	maxTimeout     = 10
)

type Consumer struct {
	committer  *kafkaCommitter
	reader     *kafkaReader
	supervisor *suture.Supervisor
	producer   func(topic string, msg []byte) error
	topicName  string
	group      string
}

func (c *Consumer) Close() {
	if c.reader != nil && c.reader.consumer != nil {
		err := c.reader.consumer.Close()
		if err != nil {
			logger.Errorf("Failed to close Kafka consumer, err: %v", err)
		}
	}
}

func NewKafkaConsumer(topicName string, kafkaConfigProperty config.KafkaConfig) *Consumer {
	offset := latestOffset
	if kafkaConfigProperty.ConsumeOffset() == oldestOffset {
		offset = oldestOffset
	}

	configMap := &k.ConfigMap{
		"bootstrap.servers":       kafkaConfigProperty.BrokerList(),
		"group.id":                kafkaConfigProperty.ConsumerGroup(),
		"security.protocol":       "SASL_SSL",
		"sasl.mechanisms":         "PLAIN",
		"sasl.username":           kafkaConfigProperty.Username(),
		"sasl.password":           kafkaConfigProperty.Password(),
		"enable.auto.commit":      false,
		"auto.offset.reset":       offset,
		"socket.keepalive.enable": true,
	}

	consumer, err := k.NewConsumer(configMap)
	if err != nil {
		log.Fatalf(logFormat, "Error to start consumer", err)
	}

	if err = consumer.SubscribeTopics([]string{topicName}, nil); err != nil {
		log.Fatalf(logFormat, "Error to subscribe topic", err)
	}

	// create producer
	kafkaClient := kafka.NewKafkaClient(config.Kafka())

	return &Consumer{
		committer: &kafkaCommitter{
			consumer: consumer,
			context:  context.TODO(),
			ack:      make(chan *k.Message),
		},
		reader: &kafkaReader{
			consumer: consumer,
			context:  context.TODO(),
			msg:      make(chan *k.Message),
		},
		producer: func(topic string, msg []byte) error {
			return kafkaClient.Push(context.Background(), msg, topic)
		},
		supervisor: suture.NewSimple(topicName),
		topicName:  topicName,
		group:      kafkaConfigProperty.ConsumerGroup(),
	}
}

func StartMainAndRetryConsumers(topicName string, kafkaConfigProperty config.KafkaConfig, fn func(msg []byte) error, retry *config.RetryConfig, wg *sync.WaitGroup) {
	ctx := context.Background()
	consumer := NewKafkaConsumer(topicName, kafkaConfigProperty)
	wg.Add(1)
	go consumer.consume(ctx, 0, fn, wg, retry)

	if retry != nil && retry.MaxRetries > 0 && retry.Type != types.RetryTypeFifo {
		for i := 1; i <= retry.MaxRetries; i++ {
			retryTopicName := helper.GetNextRetryTopicName(topicName, i)
			retryConsumer := NewKafkaConsumer(retryTopicName, kafkaConfigProperty)
			wg.Add(1)
			go retryConsumer.consume(ctx, i, fn, wg, retry)
		}
	}
}

func (c *Consumer) consume(ctx context.Context, workerID int, fn func(msg []byte) error, wg *sync.WaitGroup, retry *config.RetryConfig) {
	defer wg.Done()
	msg, ack := c.spawnReaderCommitter(ctx)
	logger := logger.GetLogger()
	logger.Infof("Topic: %s group: %s started kafka consumer: %d", c.topicName, c.group, workerID)

	if split := strings.Split(c.topicName, types.Retry_Postfix); len(split) == 2 {
		// retry topic
		c.processRetryMessage(ctx, fn, retry, msg, ack)
	} else {
		// main topic
		c.processMessage(ctx, fn, retry, msg, ack)
	}
	c.supervisor.Stop()
}

func (c *Consumer) processMessage(ctx context.Context, fn func(msg []byte) error, retry *config.RetryConfig, msg <-chan *k.Message, ack chan<- *k.Message) {
	tick := time.NewTicker(time.Duration(1000/config.ConsumerMessagesToProcessPSec()) * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case m := <-msg:
			<-tick.C
			c.runFunc(fn, m, retry, ack)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) processRetryMessage(ctx context.Context, fn func(msg []byte) error, retry *config.RetryConfig, msg <-chan *k.Message, ack chan<- *k.Message) {
	for {
		select {
		case m := <-msg:
			c.runRetry(fn, m, retry, ack)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) runRetry(fn func(msg []byte) error, m *k.Message, retry *config.RetryConfig, ack chan<- *k.Message) {
	// get retry message from kafka message
	retryMessageData := &types.RetryMessage{}
	err := json.Unmarshal(m.Value, retryMessageData)
	if err != nil {
		logger.Errorf("Failed to unmarshal retry message, err: %v", err)
		if retry.ErrorCallback != nil {
			retry.ErrorCallback(m.Value, err)
		}
		return
	}

	// sleep till next execution time
	time.Sleep(time.Until(retryMessageData.ExpectedExecutionTime))

	// process message
	err = fn(retryMessageData.Message.Value)
	if err != nil {
		// retry
		retryMessageData.RetryCounter++
		if retryMessageData.RetryCounter > retry.MaxRetries {
			// max retry reached
			if retry.ErrorCallback != nil {
				retry.ErrorCallback(m.Value, err)
			}

			// move to dead letter topic
			dlqTopicName := helper.GetDLQTopicName(c.topicName)
			err = c.producer(dlqTopicName, retryMessageData.Message.Value)
			if err != nil {
				logger.Errorf("Failed to push message to DLQ topic: %s, err: %v", dlqTopicName, err)
				if retry.ErrorCallback != nil {
					retry.ErrorCallback(m.Value, err)
				}
			}
		} else {
			nextRetryTopic := helper.GetNextRetryTopicName(c.topicName, retryMessageData.RetryCounter)

			backOffTime := helper.GetBackOffTimeInMilliSeconds(retryMessageData.RetryCounter, retry.Type)
			retryMessageData.ExpectedExecutionTime = time.Now().Add(time.Duration(backOffTime * int(time.Millisecond)))

			nextRetryData, err := json.Marshal(retryMessageData)
			if err != nil {
				logger.Errorf("Failed to marshal retry message, err: %v", err)
				if retry.ErrorCallback != nil {
					retry.ErrorCallback(m.Value, err)
				}
				return
			}
			// push to next retry topic
			err = c.producer(nextRetryTopic, nextRetryData)
			if err != nil {
				logger.Errorf("Failed to push to retry topic, err: %v", err)
				if retry.ErrorCallback != nil {
					retry.ErrorCallback(m.Value, err)
				}
			}
		}
	}

	ack <- m
}

func (c *Consumer) runFunc(fn func(msg []byte) error, msg *k.Message, rc *config.RetryConfig, ack chan<- *k.Message) {
	// process message
	err := fn(msg.Value)

	if err != nil {
		if rc != nil {
			// retry
			err := c.Retry(fn, msg, rc)
			// trigger failed retry callback
			if err != nil {
				logger.Errorf("Failed to retry message, err: %v", err)
				if rc.ErrorCallback != nil {
					rc.ErrorCallback(msg.Value, err)
				}
			}
		} else {
			// move to DLQ
			err := c.producer(helper.GetDLQTopicName(c.topicName), msg.Value)
			if err != nil {
				logger.Errorf("Failed to move message to DLQ, err: %v", err)
			}
		}
	}

	ack <- msg
}

func (c *Consumer) spawnReaderCommitter(ctx context.Context) (<-chan *k.Message, chan<- *k.Message) {
	c.reader.context = ctx
	c.committer.context = ctx

	c.supervisor.Add(c.reader)
	c.supervisor.Add(c.committer)
	c.supervisor.ServeBackground()

	return c.reader.msg, c.committer.ack
}
