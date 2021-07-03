package app

import (
	"callisto/instrumentation"
	"callisto/logger"
)

func InitApp() {
	logger.SetupLogger()
	//kafka.init(k.config())
	instrumentation.InitSentry()
}

func StopApp() {
	instrumentation.StopSentry()
}
