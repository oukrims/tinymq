package shared

import (
	"log"
	"sync"
)

var (
	loggerMu sync.RWMutex
	logger   = log.Default()
)

func SetLogger(l *log.Logger) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	logger = l
}

func Logf(format string, v ...any) {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	logger.Printf(format, v...)
}
