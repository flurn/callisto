package app

import (
	"callisto/instrumentation"
	"callisto/logger"
)

func InitApp() {
	logger.SetupLogger()
	instrumentation.InitSentry()
}

func StopApp() {
	instrumentation.StopSentry()
}
