package test

import (
	"fmt"

	"github.com/flurn/callisto/app"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/kafka"
	"github.com/flurn/callisto/server"
)

var topic string = "test_topic"

func Test() {
	// load config
	config.Load(config.AppServer)

	// setup logger and other utils
	app.InitApp()
	defer app.StopApp()

	kafkaConfig := config.Kafka()

	kafka.CreateTopic(topic, kafkaConfig)

	// kafkaClient := kafka.NewKafkaClient(kafkaConfig)
	// for i := 0; i < 10; i++ {
	// 	err := kafkaClient.Push(context.Background(), []byte("test "+strconv.Itoa(i)), topic)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	fn := func(message []byte) error {
		fmt.Println(string(message))
		return nil
	}
	server.StartConsumerASWorker(fn)
}
