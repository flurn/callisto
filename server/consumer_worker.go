package server

// import (
// 	"context"
// 	"fmt"
// 	"net/http"
// 	"sync"

// 	"github.com/flurn/callisto/config"
// 	"github.com/flurn/callisto/kafka/consumer"
// 	"github.com/flurn/callisto/types"
// )

// func StartConsumerASWorker(fn func(msg []byte) error, topics []string) {
// 	go func() {
// 		fmt.Println("Starting server on port", config.ConsumerASWorkerPort())
// 		err := http.ListenAndServe(fmt.Sprintf(":%d", config.ConsumerASWorkerPort()), nil)
// 		if err != nil {
// 			panic("failed to start http server for profiling")
// 		}
// 	}()

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	var wg sync.WaitGroup
// 	consumerAsWorkerConfig := config.ConsumerASWorker()

// 	retryConfig := &config.RetryConfig{
// 		Type:       types.RetryTypeFifo,
// 		MaxRetries: 5,
// 		RetryFailedCallback: func(msg []byte, err error) {
// 			fmt.Println("retry failed hook")
// 		},
// 	}

// 	wg := &sync.WaitGroup{}
// 	consumer.StartMainAndRetryConsumers(topic, kafka.Config(), fn, retryConfig, wg)

// 	wg.Wait()
// }
