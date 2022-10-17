package config

import (
	"sync"

	"github.com/spf13/viper"
)

var appConfig Config
var appConfigMutex sync.RWMutex

type Config struct {
	port                            int
	sentryDSN                       string
	log                             LogConfig
	sentryEnabled                   bool
	kafkaConfig                     KafkaConfig
	consumerMessagesProcessedPerSec int
	consumerASWorkerPort            int
	consumerASWorkerConfig          KafkaConfig
	retryBackoffMs                  int
}

func Port() int {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.port
}
func Log() LogConfig {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.log
}

func ConsumerASWorkerPort() int {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.consumerASWorkerPort
}

func ConsumerASWorker() KafkaConfig {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.consumerASWorkerConfig
}

func SentryDSN() string {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.sentryDSN
}
func SentryEnabled() bool {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.sentryEnabled
}
func Kafka() KafkaConfig {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.kafkaConfig
}
func ConsumerMessagesToProcessPSec() int {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.consumerMessagesProcessedPerSec
}
func RetryBackoffMs() int {
	appConfigMutex.RLock()
	defer appConfigMutex.RUnlock()
	return appConfig.retryBackoffMs
}

type ApplicationType string

const (
	AppServer        ApplicationType = "SERVER"
	ConsumerAsWorker ApplicationType = "CONSUMER_WORKER"
)

func Load(appType ApplicationType, configFile string) {
	viper.SetDefault("APP_PORT", "8080")
	viper.SetDefault("LOG_LEVEL", "error")
	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("application")
		viper.AddConfigPath("./")
		viper.AddConfigPath("../")
		viper.AddConfigPath("../../")
		viper.SetConfigType("yaml")
	}
	viper.ReadInConfig()
	viper.AutomaticEnv()
	cfg := Config{
		port:                 mustGetInt("APP_PORT"),
		consumerASWorkerPort: mustGetInt("CONSUMER_AS_WORKER_PORT"),
		log: LogConfig{
			logLevel: mustGetString("LOG_LEVEL"),
			format:   mustGetString("LOG_FORMAT"),
		},
		sentryDSN:                       mustGetString("SENTRY_DSN"),
		sentryEnabled:                   mustGetBool("SENTRY_ENABLED"),
		consumerMessagesProcessedPerSec: mustGetInt("CONSUMER_NUM_MESSAGES_PROCESSED_PER_SECOND"),
		kafkaConfig:                     newKafkaConfig(),
		consumerASWorkerConfig:          consumerASWorkerConfig(),
		retryBackoffMs:                  mustGetInt("CONS_KAFKA_RETRY_BACKOFF_MS"),
	}
	appConfigMutex.Lock()
	defer appConfigMutex.Unlock()

	appConfig = cfg
}
