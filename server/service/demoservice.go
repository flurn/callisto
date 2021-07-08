package service

import (
	"callisto/logger"
	"net/http"
)

func PushMessage(producer *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger := logger.WithRequest(r)
		logger.Infof("Printing logs")
		producer.kafka.Push(r.Context(), []byte{}, "topicName")
	}
}
