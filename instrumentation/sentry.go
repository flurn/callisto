package instrumentation

import (
	"log"

	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/logger"
	raven "github.com/getsentry/raven-go"
)

var client *raven.Client

func InitSentry() {
	if config.SentryEnabled() {
		var err error
		client, err = raven.New(config.SentryDSN())
		if err != nil {
			log.Fatalf(err.Error())
		}

		logger.Infof("sentry started")
	}
}

func StopSentry() {
	if client != nil {
		client.Close()
		logger.Infof("sentry shutdown")
	}
}

func CaptureError(err error) {
	if config.SentryEnabled() {
		client.CaptureError(err, map[string]string{})
	}
}
