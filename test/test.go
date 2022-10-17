package test

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/flurn/callisto/app"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/kafka"
	"github.com/flurn/callisto/kafka/consumer"
	"github.com/flurn/callisto/logger"
	"github.com/flurn/callisto/types"
)

var topic string = "test_topic_5"

func Test() {
	// load config
	config.Load(config.AppServer)

	// setup logger and other utils
	app.InitApp()
	defer app.StopApp()

	topicConfig := &config.KafkaTopicConfig{
		TopicName: topic,
		Retry: &config.RetryConfig{
			Type:       types.RetryTypeCustom,
			MaxRetries: 5,
			ErrorCallback: func(msg []byte, err error) {
				logger.Error("retry failed hook")
			},
		},
		ConsumerMessagePSec: 1000,
	}

	kafkaConfig := config.Kafka()

	kafka.CreateTopic(topicConfig, kafkaConfig)

	kafkaClient := kafka.NewKafkaClient(kafkaConfig)
	for i := 0; i < 1; i++ {
		err := kafkaClient.Push(context.Background(), []byte("test "+strconv.Itoa(i)), topic)
		if err != nil {
			fmt.Println(err)
		}
	}

	fn := func(message []byte) error {
		logger.Error("message: ", string(message))
		// just return error for testing exponential backOff
		// if rand.Intn(2) == 1 {
		// 	return fmt.Errorf("error")
		// }

		// fmt.Println(string(message))
		return fmt.Errorf("error")
	}

	wg := &sync.WaitGroup{}
	consumer.StartMainAndRetryConsumers(topic, kafkaConfig, fn, topicConfig.Retry, wg)
	wg.Wait()
}
