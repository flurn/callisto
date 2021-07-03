package config

import (
	"sync"

	"github.com/spf13/viper"
)

var appConfig Config
var appConfigMutex sync.RWMutex

type Config struct {
	port          int
	sentryDSN     string
	log           LogConfig
	sentryEnabled bool
	kafkaConfig   KafkaConfig
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

type ApplicationType string

const (
	AppServer ApplicationType = "SERVER"
)

func Load(appType ApplicationType) {
	viper.SetDefault("APP_PORT", "8080")
	viper.SetDefault("LOG_LEVEL", "error")

	viper.SetConfigName("application")
	viper.AddConfigPath("./")
	viper.AddConfigPath("../")
	viper.AddConfigPath("../../")
	viper.SetConfigType("yaml")

	viper.ReadInConfig()
	viper.AutomaticEnv()

	cfg := Config{
		port: mustGetInt("APP_PORT"),
		log: LogConfig{
			logLevel: mustGetString("LOG_LEVEL"),
			format:   mustGetString("LOG_FORMAT"),
		},
		sentryDSN:     mustGetString("SENTRY_DSN"),
		sentryEnabled: mustGetBool("SENTRY_ENABLED"),
		kafkaConfig:   newKafkaConfig(),
	}
	appConfigMutex.Lock()
	defer appConfigMutex.Unlock()

	appConfig = cfg
}
