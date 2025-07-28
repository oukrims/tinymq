package queue

import (
	"sync"
	"testing"
	"time"
)

func TestCustomSchedulerInterval(t *testing.T) {
	config := Config{
		BufferSize:        10,
		SchedulerInterval: 10 * time.Millisecond, // Much faster than default
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]string, 0)

	q.RegisterExecutor("scheduler-test", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Payload.(string))
		mu.Unlock()
		return nil
	})

	q.StartDispatcher(1)

	job := NewJob("scheduler-test", "fast-scheduled")
	q.EnqueueDelayed(job, 50*time.Millisecond)

	// With 10ms scheduler interval, job should execute quickly after 50ms
	time.Sleep(80 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 1 {
		t.Fatalf("Expected 1 job executed, got %d", len(executed))
	}

	if executed[0] != "fast-scheduled" {
		t.Errorf("Expected 'fast-scheduled', got %s", executed[0])
	}
}

func TestDefaultSchedulerInterval(t *testing.T) {
	q := NewJobQueue(10) // Uses default config
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]string, 0)

	q.RegisterExecutor("default-test", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Payload.(string))
		mu.Unlock()
		return nil
	})

	q.StartDispatcher(1)

	job := NewJob("default-test", "default-scheduled")
	q.EnqueueDelayed(job, 150*time.Millisecond)

	// Should execute after delay + some scheduler intervals
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 1 {
		t.Fatalf("Expected 1 job executed, got %d", len(executed))
	}
}

func TestSchedulerPrecision(t *testing.T) {
	config := Config{
		BufferSize:        10,
		SchedulerInterval: 1 * time.Millisecond, // Very precise
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	var mu sync.Mutex
	var executedAt time.Time

	q.RegisterExecutor("precision-test", func(job Job) error {
		mu.Lock()
		executedAt = time.Now()
		mu.Unlock()
		return nil
	})

	q.StartDispatcher(1)

	scheduledAt := time.Now()
	delay := 25 * time.Millisecond
	job := NewJob("precision-test", "precise")
	q.EnqueueDelayed(job, delay)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if executedAt.IsZero() {
		t.Fatal("Job was not executed")
	}

	actualDelay := executedAt.Sub(scheduledAt)
	// Should execute within a few milliseconds of the target
	if actualDelay < delay || actualDelay > delay+10*time.Millisecond {
		t.Errorf("Expected execution around %v, got %v", delay, actualDelay)
	}
}
