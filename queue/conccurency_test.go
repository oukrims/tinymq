package queue

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutorConcurrencyLimits(t *testing.T) {
	q := NewJobQueue(100)
	defer q.Stop()

	var activeJobs int64
	var maxConcurrent int64

	execConfig := ExecutorConfig{
		MaxConcurrentJobs: 3,
		Timeout:           time.Second,
	}

	q.RegisterExecutorWithConfig("concurrent-test", func(job Job) error {
		current := atomic.AddInt64(&activeJobs, 1)
		defer atomic.AddInt64(&activeJobs, -1)

		// Track maximum concurrent jobs
		for {
			max := atomic.LoadInt64(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt64(&maxConcurrent, max, current) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
		return nil
	}, execConfig)

	q.StartDispatcher(10) // Many workers to test concurrency limiting

	// Enqueue many jobs quickly
	for i := 0; i < 20; i++ {
		job := NewJob("concurrent-test", i)
		q.Enqueue(job)
	}

	time.Sleep(500 * time.Millisecond)

	finalMax := atomic.LoadInt64(&maxConcurrent)
	if finalMax > 3 {
		t.Errorf("Expected max concurrent jobs to be 3, got %d", finalMax)
	}
}

func TestExecutorTimeout(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	var jobCompleted bool
	var mu sync.Mutex

	execConfig := ExecutorConfig{
		MaxConcurrentJobs: 5,
		Timeout:           50 * time.Millisecond,
	}

	q.RegisterExecutorWithConfig("timeout-test", func(job Job) error {
		time.Sleep(200 * time.Millisecond) // Longer than timeout
		mu.Lock()
		jobCompleted = true
		mu.Unlock()
		return nil
	}, execConfig)

	q.StartDispatcher(1)

	job := NewJob("timeout-test", "should-timeout")
	q.Enqueue(job)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if jobCompleted {
		t.Error("Expected job to be cancelled due to timeout, but it completed")
	}
}

func TestConcurrencyRequeuing(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]int, 0)

	execConfig := ExecutorConfig{
		MaxConcurrentJobs: 2,
		Timeout:           time.Second,
	}

	q.RegisterExecutorWithConfig("requeue-test", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Payload.(int))
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return nil
	}, execConfig)

	q.StartDispatcher(1)

	// Enqueue more jobs than concurrency limit
	for i := 0; i < 5; i++ {
		job := NewJob("requeue-test", i)
		q.Enqueue(job)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 5 {
		t.Errorf("Expected 5 jobs executed, got %d", len(executed))
	}
}

func TestPersistenceErrorHandling(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	// Mock persistence that always fails
	failingStore := &MockPersistence{
		shouldFailSave: true,
	}
	q.SetPersistence(failingStore)

	job := NewJob("test", "should-fail-to-persist")
	err := q.Enqueue(job)

	if err == nil {
		t.Error("Expected error from failing persistence, got nil")
	}
}

func TestPersistenceRetryErrorHandling(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	// Mock persistence that fails on delete
	failingStore := &MockPersistence{
		shouldFailDelete: true,
	}
	q.SetPersistence(failingStore)

	q.RegisterExecutor("retry-persist-test", func(job Job) error {
		return errors.New("simulated failure")
	})

	q.StartDispatcher(1)

	job := NewJob("retry-persist-test", "test-retry-persistence")
	job.RetryConfig.MaxRetries = 1

	q.Enqueue(job)
	time.Sleep(200 * time.Millisecond)

	// Should not crash despite persistence errors
}

type MockPersistence struct {
	shouldFailSave   bool
	shouldFailDelete bool
	mu               sync.Mutex
	jobs             map[string]Job
}

func (m *MockPersistence) Save(job Job) error {
	if m.shouldFailSave {
		return errors.New("mock save failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jobs == nil {
		m.jobs = make(map[string]Job)
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *MockPersistence) Delete(jobID string) error {
	if m.shouldFailDelete {
		return errors.New("mock delete failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, jobID)
	return nil
}

func (m *MockPersistence) LoadAll() ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobs := make([]Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	return jobs, nil
}
