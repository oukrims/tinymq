package queue

import (
	"container/heap"
	"context"
	"fmt"
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

type executorInfo struct {
	fn        ExecutorFunc
	config    ExecutorConfig
	semaphore chan struct{}
}

type JobQueue struct {
	highQueue         chan Job
	mediumQueue       chan Job
	lowQueue          chan Job
	executors         map[string]*executorInfo
	mu                sync.RWMutex
	workers           int
	store             Persistence
	delayedJobs       delayedQueue
	delayedMu         sync.Mutex
	stopCh            chan struct{}
	schedulerInterval time.Duration
	maxDelayedJobs    int
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
		executors:         make(map[string]*executorInfo),
		workers:           1,
		delayedJobs:       make(delayedQueue, 0),
		stopCh:            make(chan struct{}),
		schedulerInterval: config.SchedulerInterval,
		maxDelayedJobs:    config.MaxDelayedJobs,
	}
	heap.Init(&jq.delayedJobs)
	go jq.schedulerLoop()
	return jq
}

func (jq *JobQueue) SetPersistence(p Persistence) {
	jq.store = p
}

func (jq *JobQueue) RegisterExecutor(jobType string, executor ExecutorFunc) {
	jq.RegisterExecutorWithConfig(jobType, executor, DefaultExecutorConfig())
}

func (jq *JobQueue) RegisterExecutorWithConfig(jobType string, executor ExecutorFunc, config ExecutorConfig) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	jq.executors[jobType] = &executorInfo{
		fn:        executor,
		config:    config,
		semaphore: make(chan struct{}, config.MaxConcurrentJobs),
	}
	shared.Logf("Registered executor for type: %s (max concurrent: %d)", jobType, config.MaxConcurrentJobs)
}

func (jq *JobQueue) Enqueue(job Job) error {
	if job.RunAt.After(time.Now()) {
		return jq.enqueueDelayed(job)
	}

	if jq.store != nil {
		if err := jq.store.Save(job); err != nil {
			shared.Logf("Failed to persist job %s: %v", job.ID, err)
			return fmt.Errorf("failed to persist job: %w", err)
		}
	}

	jq.enqueueByPriority(job)
	shared.Logf("Job enqueued with priority %v: %s", job.Priority, job.ID)
	return nil
}

func (jq *JobQueue) EnqueueDelayed(job Job, delay time.Duration) error {
	job.RunAt = time.Now().Add(delay)
	return jq.enqueueDelayed(job)
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

func (jq *JobQueue) enqueueDelayed(job Job) error {
	jq.delayedMu.Lock()
	defer jq.delayedMu.Unlock()

	if jq.store != nil {
		if err := jq.store.Save(job); err != nil {
			shared.Logf("Failed to persist delayed job %s: %v", job.ID, err)
			return fmt.Errorf("failed to persist delayed job: %w", err)
		}
	}

	// Check if we have a limit (0 means unlimited)
	if jq.maxDelayedJobs > 0 {
		// Check if we're at capacity
		if jq.delayedJobs.Len() >= jq.maxDelayedJobs {
			// Remove oldest job (earliest RunAt time)
			if jq.delayedJobs.Len() > 0 {
				oldest := heap.Pop(&jq.delayedJobs).(*scheduledJob)
				shared.Logf("Delayed jobs at capacity (%d), dropping oldest job: %s (was scheduled for %v)",
					jq.maxDelayedJobs, oldest.job.ID, oldest.runAt)
			}
		}

		// Warn when approaching capacity
		if jq.delayedJobs.Len() >= int(float64(jq.maxDelayedJobs)*0.9) {
			shared.Logf("Delayed jobs queue approaching capacity: %d/%d", jq.delayedJobs.Len(), jq.maxDelayedJobs)
		}
	}

	heap.Push(&jq.delayedJobs, &scheduledJob{
		job:   job,
		runAt: job.RunAt,
	})
	shared.Logf("Job scheduled for %v: %s", job.RunAt, job.ID)
	return nil
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

func (jq *JobQueue) selectNextJob() (Job, bool) {
	// Always check high priority first
	select {
	case job, ok := <-jq.highQueue:
		return job, ok
	default:
	}

	// If no high priority job, check high and medium
	select {
	case job, ok := <-jq.highQueue:
		return job, ok
	case job, ok := <-jq.mediumQueue:
		return job, ok
	default:
	}

	// If no high or medium priority job, check all queues
	select {
	case job, ok := <-jq.highQueue:
		return job, ok
	case job, ok := <-jq.mediumQueue:
		return job, ok
	case job, ok := <-jq.lowQueue:
		return job, ok
	}
}

func (jq *JobQueue) worker(id int) {
	for {
		job, ok := jq.selectNextJob()
		if !ok {
			return
		}

		jq.mu.RLock()
		executorInfo, exists := jq.executors[job.Type]
		jq.mu.RUnlock()

		if exists {
			go jq.executeJob(id, job, executorInfo)
		} else {
			shared.Logf("[Worker %d] No executor found for job type '%s'", id, job.Type)
		}
	}
}

func (jq *JobQueue) executeJob(workerID int, job Job, execInfo *executorInfo) {
	// Acquire semaphore to limit concurrent executions
	select {
	case execInfo.semaphore <- struct{}{}:
		defer func() { <-execInfo.semaphore }()
	default:
		// Semaphore full, re-queue the job with a small delay
		shared.Logf("[Worker %d] Executor for %s at capacity, re-queuing job: %s", workerID, job.Type, job.ID)
		job.RunAt = time.Now().Add(100 * time.Millisecond)
		jq.enqueueDelayed(job)
		return
	}

	shared.Logf("[Worker %d] Executing %v priority job: %s (attempt %d)",
		workerID, job.Priority, job.ID, job.Retries+1)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), execInfo.config.Timeout)
	defer cancel()

	// Execute with timeout
	done := make(chan error, 1)
	go func() {
		done <- execInfo.fn(job)
	}()

	select {
	case err := <-done:
		if err != nil {
			jq.handleJobFailure(job, err)
		} else {
			jq.handleJobSuccess(job)
		}
	case <-ctx.Done():
		err := fmt.Errorf("job execution timed out after %v", execInfo.config.Timeout)
		shared.Logf("[Worker %d] Job %s timed out", workerID, job.ID)
		jq.handleJobFailure(job, err)
	}
}

func (jq *JobQueue) handleJobSuccess(job Job) {
	shared.Logf("Job completed successfully: %s", job.ID)
	if jq.store != nil {
		if err := jq.store.Delete(job.ID); err != nil {
			shared.Logf("Failed to delete completed job %s from persistence: %v", job.ID, err)
		}
	}
}

func (jq *JobQueue) handleJobFailure(job Job, err error) {
	job.LastError = err.Error()
	job.Retries++

	if job.Retries > job.RetryConfig.MaxRetries {
		shared.Logf("Job failed permanently after %d retries: %s - %v", job.RetryConfig.MaxRetries, job.ID, err)
		if jq.store != nil {
			if deleteErr := jq.store.Delete(job.ID); deleteErr != nil {
				shared.Logf("Failed to delete failed job %s from persistence: %v", job.ID, deleteErr)
			}
		}
		return
	}

	retryDelay := job.calculateRetryDelay()
	job.RunAt = time.Now().Add(retryDelay)

	shared.Logf("Job failed, scheduling retry %d/%d in %v: %s - %v",
		job.Retries, job.RetryConfig.MaxRetries, retryDelay, job.ID, err)

	if retryErr := jq.enqueueDelayed(job); retryErr != nil {
		shared.Logf("Failed to schedule retry for job %s: %v", job.ID, retryErr)
	}
}
