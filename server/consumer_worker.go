package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/kafka/consumer"
	"github.com/flurn/callisto/types"
)

func StartConsumerASWorker(fn func(msg []byte) error, topics []string) {

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

	retryConfig := &types.RetryConfig{
		Type:       types.RetryTypeFifo,
		MaxRetries: 5,
		RetryFailedCallback: func(msg []byte, err error) {
			fmt.Println("retry failed hook")
		},
	}

	for _, topic := range topics {
		for wID := 0; wID < consumerAsWorkerConfig.Workers(); wID++ {
			kafkaConsumer := consumer.NewKafkaConsumer(topic, consumerAsWorkerConfig)
			defer kafkaConsumer.Close()
			go kafkaConsumer.Consume(ctx, wID, fn, &wg, retryConfig)
			wg.Add(1)
		}
	}

	wg.Wait()
}
