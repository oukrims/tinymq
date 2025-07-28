package shared

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestLoggerOverride(t *testing.T) {
	var buf bytes.Buffer
	custom := log.New(&buf, "", 0)
	SetLogger(custom)
	Logf("hello %s", "world")

	if !strings.Contains(buf.String(), "hello world") {
		t.Errorf("expected log output to contain 'hello world', got: %s", buf.String())
	}
}
