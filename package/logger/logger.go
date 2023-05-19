package logger

import (
	"log"
	"os"

	"github.com/sirupsen/logrus"
	easy "github.com/t-tomalak/logrus-easy-formatter"
)

var Nodelogger *logrus.Logger

func Logger_init(path string) {
	Nodelogger = logrus.New()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("open file error=%v", err)
	}
	formatter := &easy.Formatter{
		TimestampFormat: "2006-01-02 15:04:05",
		LogFormat:       "[%lvl%]: %time% - %msg%\n",
	}
	Nodelogger.SetFormatter(formatter)
	Nodelogger.SetLevel(logrus.InfoLevel)
	Nodelogger.SetOutput(f)
}

func CheckError(err error) bool {
	if err != nil {
		Nodelogger.Errorf("%s", err.Error())
		return false
	}
	return true
}

func CheckFatal(err error) {
	if err != nil {
		// fmt.Fprintf(os.Stderr, "%s error: %s", function, err.Error())
		Nodelogger.Fatalf("%s", err.Error())
		os.Exit(1)
	}
}
func CheckWarn(err error) bool {
	if err != nil {
		Nodelogger.Warnf("%s", err.Error())
		return false
	}
	return true
}
