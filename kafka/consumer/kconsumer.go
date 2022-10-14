package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/thejerf/suture"

	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/kafka"
	"github.com/flurn/callisto/logger"
	"github.com/flurn/callisto/retry"
)

const (
	logFormat    = "[Consumer]: %s, %+v"
	oldestOffset = "earliest"
	latestOffset = "latest"

	exponentFactor = 2
	initialTimeout = 1
	maxTimeout     = 10
)

type processFn func(msg []byte) error

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
	moveToDLQ  func(msg []byte, topicName, groupName string) error
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
		moveToDLQ: func(msg []byte, topicName, groupName string) error {
			return kafkaClient.Push(context.Background(), msg, topicName+"-dlq")
		},
		supervisor: suture.NewSimple(topicName),
		topicName:  topicName,
		group:      kafkaConfigProperty.ConsumerGroup(),
	}
}

func (c *Consumer) Consume(ctx context.Context, workerID int, fn func(msg []byte) error, wg *sync.WaitGroup) {
	defer wg.Done()
	msg, ack := c.spawnReaderCommitter(ctx)
	logger := logger.GetLogger()
	logger.Infof("Topic: %s group: %s started kafka consumer: %d", c.topicName, c.group, workerID)
	c.processMessage(ctx, fn, msg, ack)
	c.supervisor.Stop()
}

func (c *Consumer) processMessage(ctx context.Context, fn func(msg []byte) error, msg <-chan *k.Message, ack chan<- *k.Message) {
	tick := time.NewTicker(time.Duration(1000/config.ConsumerMessagesToProcessPSec()) * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case m := <-msg:
			<-tick.C
			c.runFuncTillDone(fn, m, ack)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) runFuncTillDone(fn func(msg []byte) error, msg *k.Message, ack chan<- *k.Message) {
	maxRetries := 5
	backOff := retry.NewExponentialBackOff(initialTimeout, maxTimeout, exponentFactor)
	for {
		err := fn(msg.Value)

		// break on max retries
		if backOff.GetRetryCounter() > maxRetries {
			logger.Errorf("Max retries reached for message: %s", string(msg.Value))

			// move to DLQ
			err = MoveToDLQ(msg.Value, c.topicName, c.group)
			if err != nil {
				logger.Errorf("Failed to move message to DLQ, err: %v", err)
				//TODO: send message to slack channel
			}

			ack <- msg
			return
		}

		if err != nil {
			time.Sleep(backOff.Next())
		} else {
			break
		}
	}
	ack <- msg
}

func MoveToDLQ(msg []byte, topicName, groupName string) error {

	return nil
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
