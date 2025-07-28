package queue

import (
	"time"

	"github.com/google/uuid"
)

type Priority int

const (
	Low Priority = iota
	Medium
	High
)

type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffMultiple float64
}

var DefaultRetryConfig = RetryConfig{
	MaxRetries:      3,
	InitialDelay:    time.Second,
	MaxDelay:        5 * time.Minute,
	BackoffMultiple: 2.0,
}

type Job struct {
	ID          string
	Type        string
	Payload     any
	Priority    Priority
	Tags        map[string]string
	Timestamp   time.Time
	RunAt       time.Time
	Retries     int
	RetryConfig RetryConfig
	LastError   string
}

func NewJob(jobType string, payload any) Job {
	return Job{
		ID:          uuid.NewString(),
		Type:        jobType,
		Payload:     payload,
		Priority:    Medium,
		Tags:        make(map[string]string),
		Timestamp:   time.Now(),
		RunAt:       time.Now(),
		Retries:     0,
		RetryConfig: DefaultRetryConfig,
	}
}

func (j *Job) WithRetryConfig(config RetryConfig) *Job {
	j.RetryConfig = config
	return j
}

func (j *Job) WithMaxRetries(maxRetries int) *Job {
	j.RetryConfig.MaxRetries = maxRetries
	return j
}

func (j *Job) calculateRetryDelay() time.Duration {
	if j.Retries == 0 {
		return j.RetryConfig.InitialDelay
	}

	delay := j.RetryConfig.InitialDelay
	for i := 0; i < j.Retries; i++ {
		delay = time.Duration(float64(delay) * j.RetryConfig.BackoffMultiple)
		if delay > j.RetryConfig.MaxDelay {
			return j.RetryConfig.MaxDelay
		}
	}
	return delay
}
