package log

import (
	"testing"
)

func Test_log(t *testing.T) {
	GlobalLogger.Error("this is err msg")
	GlobalLogger.Debug("this is debg msg")
	GlobalLogger.Info("this is a %s msg", "INFO")
}
