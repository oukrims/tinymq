package queue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestJobRetrySuccess(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	attempts := 0
	var mu sync.Mutex

	q.RegisterExecutor("flaky", func(job Job) error {
		mu.Lock()
		attempts++
		currentAttempt := attempts
		mu.Unlock()

		if currentAttempt < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})

	q.StartDispatcher(1)

	job := NewJob("flaky", "test")
	job.RetryConfig.InitialDelay = 50 * time.Millisecond
	q.Enqueue(job)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	finalAttempts := attempts
	mu.Unlock()

	if finalAttempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", finalAttempts)
	}
}

func TestJobRetryExhaustion(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	attempts := 0
	var mu sync.Mutex

	q.RegisterExecutor("failing", func(job Job) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return errors.New("always fails")
	})

	q.StartDispatcher(1)

	job := NewJob("failing", "test")
	job.RetryConfig.MaxRetries = 2
	job.RetryConfig.InitialDelay = 10 * time.Millisecond
	q.Enqueue(job)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	finalAttempts := attempts
	mu.Unlock()

	expectedAttempts := job.RetryConfig.MaxRetries + 1
	if finalAttempts != expectedAttempts {
		t.Errorf("Expected %d attempts, got %d", expectedAttempts, finalAttempts)
	}
}

func TestRetryDelayCalculation(t *testing.T) {
	job := NewJob("test", nil)
	job.RetryConfig = RetryConfig{
		MaxRetries:      5,
		InitialDelay:    100 * time.Millisecond,
		MaxDelay:        time.Second,
		BackoffMultiple: 2.0,
	}

	tests := []struct {
		retries  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, time.Second}, // capped at MaxDelay
		{5, time.Second}, // still capped
	}

	for _, tt := range tests {
		job.Retries = tt.retries
		delay := job.calculateRetryDelay()
		if delay != tt.expected {
			t.Errorf("Retry %d: expected delay %v, got %v", tt.retries, tt.expected, delay)
		}
	}
}

func TestJobWithCustomRetryConfig(t *testing.T) {
	job := NewJob("test", "data")

	customConfig := RetryConfig{
		MaxRetries:      5,
		InitialDelay:    2 * time.Second,
		MaxDelay:        10 * time.Second,
		BackoffMultiple: 1.5,
	}

	job.WithRetryConfig(customConfig)

	if job.RetryConfig.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", job.RetryConfig.MaxRetries)
	}
	if job.RetryConfig.InitialDelay != 2*time.Second {
		t.Errorf("Expected InitialDelay 2s, got %v", job.RetryConfig.InitialDelay)
	}
}

func TestJobWithMaxRetriesHelper(t *testing.T) {
	job := NewJob("test", "data")
	job.WithMaxRetries(10)

	if job.RetryConfig.MaxRetries != 10 {
		t.Errorf("Expected MaxRetries 10, got %d", job.RetryConfig.MaxRetries)
	}
}
