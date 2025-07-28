package queue

import (
	"sync"
	"testing"
	"time"
)

func TestJobPriorityOrdering(t *testing.T) {
	q := NewJobQueue(30)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]string, 0)

	q.RegisterExecutor("priority-test", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Payload.(string))
		mu.Unlock()
		time.Sleep(10 * time.Millisecond) // Small delay to ensure ordering
		return nil
	})

	// Add jobs in mixed order
	jobs := []Job{
		NewLowPriorityJob("priority-test", "low-1"),
		NewHighPriorityJob("priority-test", "high-1"),
		NewJob("priority-test", "medium-1"), // default medium
		NewLowPriorityJob("priority-test", "low-2"),
		NewHighPriorityJob("priority-test", "high-2"),
		NewJob("priority-test", "medium-2"),
	}

	// Enqueue all jobs quickly
	for _, job := range jobs {
		q.Enqueue(job)
	}

	// Start single worker to ensure sequential processing
	q.StartDispatcher(1)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 6 {
		t.Fatalf("Expected 6 jobs executed, got %d", len(executed))
	}

	// High priority jobs should come first
	highCount := 0
	for i, payload := range executed {
		if payload == "high-1" || payload == "high-2" {
			highCount++
			if i >= 2 { // High priority jobs should be in first 2 positions
				t.Errorf("High priority job %s executed at position %d, expected earlier", payload, i)
			}
		}
	}

	if highCount != 2 {
		t.Errorf("Expected 2 high priority jobs, found %d", highCount)
	}
}

func TestPriorityHelperMethods(t *testing.T) {
	job := NewJob("test", "data")

	if job.Priority != Medium {
		t.Errorf("Expected default priority Medium, got %v", job.Priority)
	}

	job.WithHighPriority()
	if job.Priority != High {
		t.Errorf("Expected High priority after WithHighPriority(), got %v", job.Priority)
	}

	job.WithLowPriority()
	if job.Priority != Low {
		t.Errorf("Expected Low priority after WithLowPriority(), got %v", job.Priority)
	}

	job.WithPriority(Medium)
	if job.Priority != Medium {
		t.Errorf("Expected Medium priority after WithPriority(Medium), got %v", job.Priority)
	}
}

func TestPriorityConstructors(t *testing.T) {
	highJob := NewHighPriorityJob("test", "high")
	if highJob.Priority != High {
		t.Errorf("NewHighPriorityJob: expected High, got %v", highJob.Priority)
	}

	lowJob := NewLowPriorityJob("test", "low")
	if lowJob.Priority != Low {
		t.Errorf("NewLowPriorityJob: expected Low, got %v", lowJob.Priority)
	}

	mediumJob := NewJob("test", "medium")
	if mediumJob.Priority != Medium {
		t.Errorf("NewJob: expected Medium, got %v", mediumJob.Priority)
	}
}

func TestPriorityWithDelayedJobs(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]string, 0)

	q.RegisterExecutor("delayed-priority", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Payload.(string))
		mu.Unlock()
		return nil
	})

	q.StartDispatcher(1)

	// Create delayed jobs with different priorities
	lowJob := NewLowPriorityJob("delayed-priority", "delayed-low")
	highJob := NewHighPriorityJob("delayed-priority", "delayed-high")

	// Both should be ready at roughly the same time
	q.EnqueueDelayed(lowJob, 100*time.Millisecond)
	q.EnqueueDelayed(highJob, 100*time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 2 {
		t.Fatalf("Expected 2 jobs executed, got %d", len(executed))
	}

	// High priority delayed job should execute first
	if executed[0] != "delayed-high" {
		t.Errorf("Expected high priority delayed job first, got %s", executed[0])
	}
}

func TestPriorityQueueFullHandling(t *testing.T) {
	// Small buffer to test queue full scenario
	q := NewJobQueue(3) // 1 per priority level
	defer q.Stop()

	// Don't start dispatcher to keep jobs in queue

	// Fill up high priority queue
	for i := 0; i < 2; i++ {
		job := NewHighPriorityJob("test", i)
		q.Enqueue(job)
	}

	// This should log a drop message but not crash
	job := NewHighPriorityJob("test", "should-drop")
	q.Enqueue(job)

	// Test should complete without hanging or crashing
	time.Sleep(10 * time.Millisecond)
}
