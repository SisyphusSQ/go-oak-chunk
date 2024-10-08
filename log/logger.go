package log

import "github.com/realcp1018/tinylog"

var GlobalLogger = tinylog.NewFileLogger("goc.log", tinylog.INFO)
var StreamLogger = tinylog.NewStreamLogger(tinylog.DEBUG)
