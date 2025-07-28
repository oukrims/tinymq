package tinymq

import (
	"github.com/oukrims/tinymq/queue"
	"github.com/oukrims/tinymq/shared"
)

type Queue = queue.JobQueue
type Job = queue.Job
type ExecutorFunc = queue.ExecutorFunc

var NewJob = queue.NewJob

var NewQueue = queue.NewJobQueue
var SetLogger = shared.SetLogger
