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

type kafkaReader struct {
	consumer *k.Consumer
	context  context.Context
	msg      chan *k.Message
}

type kafkaCommitter struct {
	consumer *k.Consumer
	context  context.Context
	ack      chan *k.Message
}

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
		"retry.backoff.ms":        time.Duration(kafkaConfigProperty.BackOff()),
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

func (c *Consumer) Consume(ctx context.Context, workerID int, fn func(msg []byte) error, wg *sync.WaitGroup, retry *types.RetryConfig) {
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

func (c *Consumer) processMessage(ctx context.Context, fn func(msg []byte) error, retry *types.RetryConfig, msg <-chan *k.Message, ack chan<- *k.Message) {
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

func (c *Consumer) processRetryMessage(ctx context.Context, fn func(msg []byte) error, retry *types.RetryConfig, msg <-chan *k.Message, ack chan<- *k.Message) {
	for {
		select {
		case m := <-msg:
			c.runRetry(fn, m, retry, ack)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) runRetry(fn func(msg []byte) error, m *k.Message, retry *types.RetryConfig, ack chan<- *k.Message) {
	// get retry message from kafka message
	retryMessageData := &types.RetryMessage{}
	err := json.Unmarshal(m.Value, retryMessageData)
	if err != nil {
		logger.Errorf("Failed to unmarshal retry message, err: %v", err)
		retry.RetryFailedCallback(m.Value, err)
		return
	}

	// sleep till next execution time
	time.Sleep(time.Until(retryMessageData.ExpectedExecutionTime))

	// process message
	err = fn(retryMessageData.Message.Value)
	if err != nil {
		// retry
		retryMessageData.MessageCounter++
		if retryMessageData.MessageCounter > retry.MaxRetries {
			// max retry reached
			retry.RetryFailedCallback(m.Value, err)

			// move to dead letter topic
			dlqTopicName := helper.GetDLQTopicName(c.topicName)
			err = c.producer(dlqTopicName, retryMessageData.Message.Value)
			if err != nil {
				logger.Errorf("Failed to push message to DLQ topic: %s, err: %v", dlqTopicName, err)
				retry.RetryFailedCallback(m.Value, err)
			}
		} else {
			nextRetryTopic := helper.GetNextRetryTopicName(c.topicName, retryMessageData.MessageCounter)

			backOffTime := helper.GetBackOffTimeInMilliSeconds(retryMessageData.MessageCounter, retry.Type)
			retryMessageData.ExpectedExecutionTime = time.Now().Add(time.Duration(backOffTime * int(time.Millisecond)))
			// push to next retry topic
			err = c.producer(nextRetryTopic, m.Value)
			if err != nil {
				logger.Errorf("Failed to push to retry topic, err: %v", err)
				retry.RetryFailedCallback(m.Value, err)
			}
		}
	}

	ack <- m
}

func (c *Consumer) runFunc(fn func(msg []byte) error, msg *k.Message, rc *types.RetryConfig, ack chan<- *k.Message) {
	// process message
	err := fn(msg.Value)

	if err != nil {
		if rc != nil {
			// retry
			err := c.Retry(fn, msg, rc)
			// trigger failed retry callback
			if err != nil {
				logger.Errorf("Failed to retry message, err: %v", err)
				rc.RetryFailedCallback(msg.Value, err)
			}
		} else {
			// move to DLQ
			err := c.producer(c.topicName+types.DLQ_Postfix, msg.Value)
			if err != nil {
				logger.Errorf("Failed to move message to DLQ, err: %v", err)
			}
		}
	}

	ack <- msg
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

func (c *Consumer) spawnReaderCommitter(ctx context.Context) (<-chan *k.Message, chan<- *k.Message) {
	c.reader.context = ctx
	c.committer.context = ctx

	c.supervisor.Add(c.reader)
	c.supervisor.Add(c.committer)
	c.supervisor.ServeBackground()

	return c.reader.msg, c.committer.ack
}
