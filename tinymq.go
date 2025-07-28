package tinymq

import (
	"time"

	"github.com/oukrims/tinymq/queue"
	"github.com/oukrims/tinymq/shared"
)

type Queue = queue.JobQueue
type Job = queue.Job
type ExecutorFunc = queue.ExecutorFunc

var NewJob = queue.NewJob

var NewQueue = queue.NewJobQueue
var SetLogger = shared.SetLogger

func NewDelayedJob(jobType string, payload any, delay time.Duration) Job {
	job := queue.NewJob(jobType, payload)
	job.RunAt = time.Now().Add(delay)
	return job
}
