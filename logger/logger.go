package logger

import (
	"log"
	"net/http"
	"os"

	"github.com/flurn/callisto/config"
	"github.com/sirupsen/logrus"
)

const (
	clientHeader   = "client_id"
	jsonLoggerType = "json"
)

var logger *logrus.Logger

func SetupLogger() {
	level, err := logrus.ParseLevel(config.Log().LogLevel())
	if err != nil {
		log.Fatalf(err.Error())
	}

	logger = &logrus.Logger{
		Out:   os.Stdout,
		Hooks: make(logrus.LevelHooks),
		Level: level,
	}

	if config.Log().LogFormat() == jsonLoggerType {
		logger.Formatter = &logrus.JSONFormatter{}
	} else {
		logger.Formatter = &logrus.TextFormatter{}
	}

	return
}

func AddHook(hook logrus.Hook) {
	logger.Hooks.Add(hook)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Debugln(args ...interface{}) {
	logger.Debugln(args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Errorln(args ...interface{}) {
	logger.Errorln(args...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Infoln(args ...interface{}) {
	logger.Infoln(args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Warnln(args ...interface{}) {
	logger.Warnln(args...)
}

func WithField(key string, value interface{}) *logrus.Entry {
	return logger.WithField(key, value)
}

func WithFields(fields logrus.Fields) *logrus.Entry {
	return logger.WithFields(fields)
}

func WithRequest(r *http.Request) *logrus.Entry {
	fields := logrus.Fields{
		"Method": r.Method,
		"Host":   r.Host,
		"Path":   r.URL.Path,
	}
	if len(r.Header.Get(clientHeader)) > 0 {
		fields["Client"] = r.Header.Get(clientHeader)
	}
	return logger.WithFields(fields)
}

func GetLogger() *logrus.Logger {
	return logger
}
