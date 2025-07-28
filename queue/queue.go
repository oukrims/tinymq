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
	highQueue         chan Job
	mediumQueue       chan Job
	lowQueue          chan Job
	executors         map[string]ExecutorFunc
	mu                sync.RWMutex
	workers           int
	store             Persistence
	delayedJobs       delayedQueue
	delayedMu         sync.Mutex
	stopCh            chan struct{}
	schedulerInterval time.Duration
}

func NewJobQueue(bufferSize int) *JobQueue {
	config := NewConfigWithBufferSize(bufferSize)
	return NewJobQueueWithConfig(config)
}

func NewJobQueueWithConfig(config Config) *JobQueue {
	jq := &JobQueue{
		highQueue:         make(chan Job, config.BufferSize/3+config.BufferSize%3),
		mediumQueue:       make(chan Job, config.BufferSize/3),
		lowQueue:          make(chan Job, config.BufferSize/3),
		executors:         make(map[string]ExecutorFunc),
		workers:           1,
		delayedJobs:       make(delayedQueue, 0),
		stopCh:            make(chan struct{}),
		schedulerInterval: config.SchedulerInterval,
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

	jq.enqueueByPriority(job)
	shared.Logf("Job enqueued with priority %v: %s", job.Priority, job.ID)
}

func (jq *JobQueue) EnqueueDelayed(job Job, delay time.Duration) {
	job.RunAt = time.Now().Add(delay)
	jq.enqueueDelayed(job)
}

func (jq *JobQueue) enqueueByPriority(job Job) {
	switch job.Priority {
	case High:
		select {
		case jq.highQueue <- job:
		default:
			shared.Logf("High priority queue full, dropping job: %s", job.ID)
		}
	case Medium:
		select {
		case jq.mediumQueue <- job:
		default:
			shared.Logf("Medium priority queue full, dropping job: %s", job.ID)
		}
	case Low:
		select {
		case jq.lowQueue <- job:
		default:
			shared.Logf("Low priority queue full, dropping job: %s", job.ID)
		}
	}
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
	ticker := time.NewTicker(jq.schedulerInterval)
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

		if jq.tryEnqueueByPriority(scheduledJob.job) {
			shared.Logf("Delayed job moved to priority queue: %s", scheduledJob.job.ID)
		} else {
			heap.Push(&jq.delayedJobs, scheduledJob)
			shared.Logf("Priority queues full, rescheduling job: %s", scheduledJob.job.ID)
			return
		}
	}
}

func (jq *JobQueue) tryEnqueueByPriority(job Job) bool {
	switch job.Priority {
	case High:
		select {
		case jq.highQueue <- job:
			return true
		default:
			return false
		}
	case Medium:
		select {
		case jq.mediumQueue <- job:
			return true
		default:
			return false
		}
	case Low:
		select {
		case jq.lowQueue <- job:
			return true
		default:
			return false
		}
	}
	return false
}

func (jq *JobQueue) StartDispatcher(workerCount int) {
	jq.workers = workerCount
	for i := 0; i < workerCount; i++ {
		go jq.worker(i)
	}
}

func (jq *JobQueue) Stop() {
	close(jq.stopCh)
	close(jq.highQueue)
	close(jq.mediumQueue)
	close(jq.lowQueue)
}

func (jq *JobQueue) worker(id int) {
	for {
		var job Job
		var ok bool

		select {
		case job, ok = <-jq.highQueue:
			if !ok {
				return
			}
		default:
			select {
			case job, ok = <-jq.highQueue:
				if !ok {
					return
				}
			case job, ok = <-jq.mediumQueue:
				if !ok {
					return
				}
			default:
				select {
				case job, ok = <-jq.highQueue:
					if !ok {
						return
					}
				case job, ok = <-jq.mediumQueue:
					if !ok {
						return
					}
				case job, ok = <-jq.lowQueue:
					if !ok {
						return
					}
				}
			}
		}

		jq.mu.RLock()
		executor, exists := jq.executors[job.Type]
		jq.mu.RUnlock()

		if exists {
			go func(j Job) {
				shared.Logf("[Worker %d] Executing %v priority job: %s (attempt %d)",
					id, j.Priority, j.ID, j.Retries+1)
				err := executor(j)

				if err != nil {
					jq.handleJobFailure(j, err)
				} else {
					jq.handleJobSuccess(j)
				}
			}(job)
		} else {
			shared.Logf("[Worker %d] No executor found for job type '%s'", id, job.Type)
		}
	}
}

func (jq *JobQueue) handleJobSuccess(job Job) {
	shared.Logf("Job completed successfully: %s", job.ID)
	if jq.store != nil {
		_ = jq.store.Delete(job.ID)
	}
}

func (jq *JobQueue) handleJobFailure(job Job, err error) {
	job.LastError = err.Error()
	job.Retries++

	if job.Retries > job.RetryConfig.MaxRetries {
		shared.Logf("Job failed permanently after %d retries: %s - %v", job.RetryConfig.MaxRetries, job.ID, err)
		if jq.store != nil {
			_ = jq.store.Delete(job.ID)
		}
		return
	}

	retryDelay := job.calculateRetryDelay()
	job.RunAt = time.Now().Add(retryDelay)

	shared.Logf("Job failed, scheduling retry %d/%d in %v: %s - %v",
		job.Retries, job.RetryConfig.MaxRetries, retryDelay, job.ID, err)

	jq.enqueueDelayed(job)
}
