package service

import "callisto/kafka"

type Service struct {
	kafka kafka.Client
}

func NewService(kafka kafka.Client) *Service {
	return &Service{
		kafka: kafka,
	}
}
