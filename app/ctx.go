package app

import (
	"github.com/flurn/callisto/instrumentation"
	"github.com/flurn/callisto/logger"
)

func InitApp() {
	logger.SetupLogger()
	instrumentation.InitSentry()
}

func StopApp() {
	instrumentation.StopSentry()
}
