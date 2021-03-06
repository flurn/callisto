package config

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestShouldLoadConfigFromFile(t *testing.T) {
	Load(AppServer)

	assert.NotNil(t, Port())
	assert.NotEmpty(t, Log().LogLevel())
}

func TestShouldLoadConfigFromEnvironment(t *testing.T) {
	configEnv := map[string]string{
		"APP_PORT":  "8181",
		"LOG_LEVEL": "debug",
	}

	for k, v := range configEnv {
		err := os.Setenv(k, v)
		assert.NoError(t, err, "unable to set os env for "+k)
	}

	Load(AppServer)

	assert.Equal(t, 8181, Port())
	assert.Equal(t, "debug", Log().LogLevel())

	kc := newKafkaConfig()
	assert.Equal(t, "localhost:9092", kc.BrokerList())
	assert.Equal(t, 50, kc.MaxConnections())
	assert.Equal(t, "sample-message-bulk", kc.BulkPublishTopic())
	assert.Equal(t, "app-message-internal", kc.InternalPublishTopic())
	assert.Equal(t, "public-txns", kc.TransactionalPublishTopic())
	assert.Equal(t, 1000, kc.BackOff())
	assert.Equal(t, "latest", kc.ConsumeOffset())
	assert.Equal(t, "service-consumer-group", kc.ConsumerGroup())
	assert.Equal(t, 1, kc.Workers())
}
