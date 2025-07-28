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

type Job struct {
	ID        string
	Type      string
	Payload   any
	Priority  Priority
	Tags      map[string]string
	Timestamp time.Time
	RunAt     time.Time
	Retries   int
}

func NewJob(jobType string, payload any) Job {
	return Job{
		ID:        uuid.NewString(),
		Type:      jobType,
		Payload:   payload,
		Priority:  Medium,
		Tags:      make(map[string]string),
		Timestamp: time.Now(),
		RunAt:     time.Now(),
		Retries:   0,
	}
}
