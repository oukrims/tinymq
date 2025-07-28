package queue

import (
	"sync"

	"github.com/oukrims/tinymq/shared"
)

type Persistence interface {
	Save(job Job) error
	Delete(jobID string) error
	LoadAll() ([]Job, error)
}

type JobQueue struct {
	queue     chan Job
	executors map[string]ExecutorFunc
	mu        sync.RWMutex
	workers   int
	store     Persistence
}

func NewJobQueue(bufferSize int) *JobQueue {
	return &JobQueue{
		queue:     make(chan Job, bufferSize),
		executors: make(map[string]ExecutorFunc),
		workers:   1,
	}
}

func (jq *JobQueue) SetPersistence(p Persistence) {
	jq.store = p
}

func (jq *JobQueue) RegisterExecutor(jobType string, executor ExecutorFunc) {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	jq.executors[jobType] = executor
	shared.Logf("Registered executor for type: %s", jobType)
}

func (jq *JobQueue) Enqueue(job Job) {
	if jq.store != nil {
		_ = jq.store.Save(job) // ignore error for now
	}
	jq.queue <- job
	shared.Logf("Job enqueued: %s", job.ID)
}

func (jq *JobQueue) StartDispatcher(workerCount int) {
	jq.workers = workerCount
	for i := 0; i < workerCount; i++ {
		go jq.worker(i)
	}
}

func (jq *JobQueue) worker(id int) {
	for job := range jq.queue {
		jq.mu.RLock()
		executor, exists := jq.executors[job.Type]
		jq.mu.RUnlock()

		if exists {
			go func(j Job) {
				shared.Logf("[Worker %d] Executing job: %s", id, j.ID)
				executor(j)
				if jq.store != nil {
					_ = jq.store.Delete(j.ID)
				}
			}(job)
		} else {
			shared.Logf("[Worker %d] No executor found for job type '%s'", id, job.Type)
		}
	}
}
