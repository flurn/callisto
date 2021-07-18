package server

import (
	"callisto/config"
	"callisto/kafka/consumer"
	"callisto/server/service"
	"context"
	"fmt"
	"net/http"
	"sync"
)

func StartConsumerASWorker() {

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
			processFn := service.WriteToDatastore()
			go kafkaConsumer.Consume(ctx, wID, processFn, &wg)
			wg.Add(1)
		}
	}

	wg.Wait()
}
