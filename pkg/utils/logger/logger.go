package logger

import (
	"github.com/sirupsen/logrus"
)

var DefaultLogger = logrus.New()

func Init(appName string) {
	DefaultLogger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	DefaultLogger.Info("Logger initialized for: ", appName)
}
