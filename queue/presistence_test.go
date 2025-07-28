package queue

import (
	"errors"
	"testing"
	"time"
)

func TestPersistenceErrorPropagation(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	store := &MockPersistence{shouldFailSave: true}
	q.SetPersistence(store)

	job := NewJob("test", "data")
	err := q.Enqueue(job)

	if err == nil {
		t.Error("Expected persistence error to be propagated")
	}

	if !errors.Is(err, errors.New("mock save failure")) {
		t.Errorf("Expected wrapped persistence error, got: %v", err)
	}
}

func TestDelayedJobPersistenceError(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	store := &MockPersistence{shouldFailSave: true}
	q.SetPersistence(store)

	job := NewJob("test", "delayed-data")
	err := q.EnqueueDelayed(job, 100*time.Millisecond)

	if err == nil {
		t.Error("Expected delayed job persistence error to be propagated")
	}
}

func TestSuccessfulPersistence(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	store := &MockPersistence{}
	q.SetPersistence(store)

	job := NewJob("test", "success-data")
	err := q.Enqueue(job)

	if err != nil {
		t.Errorf("Expected no error with successful persistence, got: %v", err)
	}

	// Check job was persisted
	jobs, err := store.LoadAll()
	if err != nil {
		t.Fatalf("Failed to load jobs: %v", err)
	}

	if len(jobs) != 1 {
		t.Errorf("Expected 1 persisted job, got %d", len(jobs))
	}

	if jobs[0].ID != job.ID {
		t.Errorf("Expected job ID %s, got %s", job.ID, jobs[0].ID)
	}
}

func TestPersistenceErrorOnJobSuccess(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	store := &MockPersistence{shouldFailDelete: true}
	q.SetPersistence(store)

	executed := false
	q.RegisterExecutor("success-test", func(job Job) error {
		executed = true
		return nil
	})

	q.StartDispatcher(1)

	job := NewJob("success-test", "test-data")
	q.Enqueue(job)

	time.Sleep(100 * time.Millisecond)

	if !executed {
		t.Error("Job should have executed despite delete failure")
	}
}

func TestPersistenceErrorOnJobFailure(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	store := &MockPersistence{shouldFailDelete: true}
	q.SetPersistence(store)

	executed := false
	q.RegisterExecutor("failure-test", func(job Job) error {
		executed = true
		return errors.New("job failed")
	})

	q.StartDispatcher(1)

	job := NewJob("failure-test", "test-data")
	job.RetryConfig.MaxRetries = 0 // Fail immediately
	q.Enqueue(job)

	time.Sleep(100 * time.Millisecond)

	if !executed {
		t.Error("Job should have executed despite delete failure")
	}
}

func TestRetrySchedulingPersistenceError(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	// Store that fails on save for retries but not initial enqueue
	store := &MockPersistence{}
	q.SetPersistence(store)

	attempts := 0
	q.RegisterExecutor("retry-persist-test", func(job Job) error {
		attempts++
		if attempts == 1 {
			// Make persistence fail for retry scheduling
			store.shouldFailSave = true
			return errors.New("first attempt failed")
		}
		return nil
	})

	q.StartDispatcher(1)

	job := NewJob("retry-persist-test", "test-data")
	job.RetryConfig.MaxRetries = 2
	q.Enqueue(job)

	time.Sleep(200 * time.Millisecond)

	// Should have attempted once, retry scheduling should have failed
	if attempts != 1 {
		t.Errorf("Expected 1 execution attempt, got %d", attempts)
	}
}
