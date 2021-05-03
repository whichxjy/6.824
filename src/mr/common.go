package mr

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
)

func setLogLevel() {
	env := os.Getenv("ENV")
	level := log.FatalLevel

	switch env {
	case "dev":
		level = log.DebugLevel
	case "test":
		level = log.ErrorLevel
	default:
		log.SetOutput(ioutil.Discard)
	}

	log.SetLevel(level)
}
