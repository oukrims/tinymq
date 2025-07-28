package tinymq

import (
	"time"

	"github.com/oukrims/tinymq/queue"
	"github.com/oukrims/tinymq/shared"
)

type Queue = queue.JobQueue
type Job = queue.Job
type ExecutorFunc = queue.ExecutorFunc
type RetryConfig = queue.RetryConfig
type Priority = queue.Priority
type Config = queue.Config

const (
	Low    = queue.Low
	Medium = queue.Medium
	High   = queue.High
)

var NewJob = queue.NewJob
var NewHighPriorityJob = queue.NewHighPriorityJob
var NewLowPriorityJob = queue.NewLowPriorityJob
var DefaultRetryConfig = queue.DefaultRetryConfig
var DefaultConfig = queue.DefaultConfig

var NewQueue = queue.NewJobQueue
var NewQueueWithConfig = queue.NewJobQueueWithConfig
var SetLogger = shared.SetLogger

func NewDelayedJob(jobType string, payload any, delay time.Duration) Job {
	job := queue.NewJob(jobType, payload)
	job.RunAt = time.Now().Add(delay)
	return job
}
