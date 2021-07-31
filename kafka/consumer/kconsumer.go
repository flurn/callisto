package consumer

import (
	"context"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/logger"
	"github.com/flurn/callisto/retry"
	"log"
	"sync"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/thejerf/suture"
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

	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":       kafkaConfigProperty.BrokerList(),
		"group.id":                kafkaConfigProperty.ConsumerGroup(),
		"enable.auto.commit":      false,
		"auto.offset.reset":       offset,
		"socket.keepalive.enable": true,
		"retry.backoff.ms":        time.Duration(kafkaConfigProperty.BackOff()),
	})
	if err != nil {
		log.Fatalf(logFormat, "Error to start consumer", err)
	}

	if err = consumer.SubscribeTopics([]string{topicName}, nil); err != nil {
		log.Fatalf(logFormat, "Error to subscribe topic", err)
	}

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
	processMessage(ctx, fn, msg, ack)
	c.supervisor.Stop()
}

func processMessage(ctx context.Context, fn func(msg []byte) error, msg <-chan *k.Message, ack chan<- *k.Message) {
	tick := time.NewTicker(time.Duration(1000/config.ConsumerMessagesToProcessPSec()) * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case m := <-msg:
			<-tick.C
			runFuncTillDone(fn, m, ack)
		case <-ctx.Done():
			return
		}
	}
}

func runFuncTillDone(fn func(msg []byte) error, msg *k.Message, ack chan<- *k.Message) {
	backoff := retry.NewExponentialBackoff(initialTimeout, maxTimeout, exponentFactor)
	for {
		err := fn(msg.Value)
		if err != nil {
			time.Sleep(backoff.Next())
		} else {
			break
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
				logger.Errorf("kafka consumer failure %v", err)
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
