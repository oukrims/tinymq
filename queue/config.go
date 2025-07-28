package queue

import "time"

type ExecutorConfig struct {
	MaxConcurrentJobs int
	Timeout           time.Duration
}

type Config struct {
	BufferSize        int
	SchedulerInterval time.Duration
	MaxDelayedJobs    int
}

func DefaultConfig() Config {
	return Config{
		BufferSize:        100,
		SchedulerInterval: 100 * time.Millisecond,
		MaxDelayedJobs:    10000,
	}
}

func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		MaxConcurrentJobs: 10,
		Timeout:           30 * time.Second,
	}
}

func NewConfigWithBufferSize(bufferSize int) Config {
	config := DefaultConfig()
	config.BufferSize = bufferSize
	return config
}
