package queue

import "time"

type Config struct {
	BufferSize        int
	SchedulerInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		BufferSize:        100,
		SchedulerInterval: 100 * time.Millisecond,
	}
}

func NewConfigWithBufferSize(bufferSize int) Config {
	config := DefaultConfig()
	config.BufferSize = bufferSize
	return config
}
