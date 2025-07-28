package queue

import (
	"container/heap"
	"sync"
	"time"

	"github.com/oukrims/tinymq/shared"
)

type Persistence interface {
	Save(job Job) error
	Delete(jobID string) error
	LoadAll() ([]Job, error)
}

type scheduledJob struct {
	job   Job
	runAt time.Time
}

type delayedQueue []*scheduledJob

func (dq delayedQueue) Len() int           { return len(dq) }
func (dq delayedQueue) Less(i, j int) bool { return dq[i].runAt.Before(dq[j].runAt) }
func (dq delayedQueue) Swap(i, j int)      { dq[i], dq[j] = dq[j], dq[i] }

func (dq *delayedQueue) Push(x interface{}) {
	*dq = append(*dq, x.(*scheduledJob))
}

func (dq *delayedQueue) Pop() interface{} {
	old := *dq
	n := len(old)
	item := old[n-1]
	*dq = old[0 : n-1]
	return item
}

type JobQueue struct {
	queue       chan Job
	executors   map[string]ExecutorFunc
	mu          sync.RWMutex
	workers     int
	store       Persistence
	delayedJobs delayedQueue
	delayedMu   sync.Mutex
	stopCh      chan struct{}
}

func NewJobQueue(bufferSize int) *JobQueue {
	jq := &JobQueue{
		queue:       make(chan Job, bufferSize),
		executors:   make(map[string]ExecutorFunc),
		workers:     1,
		delayedJobs: make(delayedQueue, 0),
		stopCh:      make(chan struct{}),
	}
	heap.Init(&jq.delayedJobs)
	go jq.schedulerLoop()
	return jq
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
	if job.RunAt.After(time.Now()) {
		jq.enqueueDelayed(job)
		return
	}

	if jq.store != nil {
		_ = jq.store.Save(job)
	}
	jq.queue <- job
	shared.Logf("Job enqueued: %s", job.ID)
}

func (jq *JobQueue) EnqueueDelayed(job Job, delay time.Duration) {
	job.RunAt = time.Now().Add(delay)
	jq.enqueueDelayed(job)
}

func (jq *JobQueue) enqueueDelayed(job Job) {
	jq.delayedMu.Lock()
	defer jq.delayedMu.Unlock()

	if jq.store != nil {
		_ = jq.store.Save(job)
	}

	heap.Push(&jq.delayedJobs, &scheduledJob{
		job:   job,
		runAt: job.RunAt,
	})
	shared.Logf("Job scheduled for %v: %s", job.RunAt, job.ID)
}

func (jq *JobQueue) schedulerLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			jq.processDelayedJobs()
		case <-jq.stopCh:
			return
		}
	}
}

func (jq *JobQueue) processDelayedJobs() {
	jq.delayedMu.Lock()
	defer jq.delayedMu.Unlock()

	now := time.Now()
	for jq.delayedJobs.Len() > 0 {
		next := jq.delayedJobs[0]
		if next.runAt.After(now) {
			break
		}

		scheduledJob := heap.Pop(&jq.delayedJobs).(*scheduledJob)
		select {
		case jq.queue <- scheduledJob.job:
			shared.Logf("Delayed job moved to queue: %s", scheduledJob.job.ID)
		default:
			heap.Push(&jq.delayedJobs, scheduledJob)
			shared.Logf("Queue full, rescheduling job: %s", scheduledJob.job.ID)
			return
		}
	}
}

func (jq *JobQueue) StartDispatcher(workerCount int) {
	jq.workers = workerCount
	for i := 0; i < workerCount; i++ {
		go jq.worker(i)
	}
}

func (jq *JobQueue) Stop() {
	close(jq.stopCh)
	close(jq.queue)
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
