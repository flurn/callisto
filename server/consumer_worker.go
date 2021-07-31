package server

import (
	"context"
	"fmt"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/kafka/consumer"
	"net/http"
	"sync"
)

func StartConsumerASWorker(fn func(msg []byte) error) {

	go func() {
		fmt.Println("Starting server on port", config.ConsumerASWorkerPort())
		err := http.ListenAndServe(fmt.Sprintf(":%d", config.ConsumerASWorkerPort()), nil)
		if err != nil {
			panic("failed to start http server for profiling")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	consumerAsWorkerConfig := config.ConsumerASWorker()

	for _, topic := range consumerAsWorkerConfig.Topics() {
		for wID := 0; wID < consumerAsWorkerConfig.Workers(); wID++ {
			kafkaConsumer := consumer.NewKafkaConsumer(topic, consumerAsWorkerConfig)
			defer kafkaConsumer.Close()
			go kafkaConsumer.Consume(ctx, wID, fn, &wg)
			wg.Add(1)
		}
	}

	wg.Wait()
}
