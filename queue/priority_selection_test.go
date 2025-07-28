package queue

import (
	"sync"
	"testing"
	"time"
)

func TestWorkerPrioritySelection(t *testing.T) {
	q := NewJobQueue(30)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]string, 0)

	q.RegisterExecutor("priority-selection-test", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Payload.(string))
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	// Fill all queues with jobs
	q.Enqueue(NewLowPriorityJob("priority-selection-test", "low-1"))
	q.Enqueue(NewJob("priority-selection-test", "medium-1"))
	q.Enqueue(NewHighPriorityJob("priority-selection-test", "high-1"))
	q.Enqueue(NewLowPriorityJob("priority-selection-test", "low-2"))
	q.Enqueue(NewJob("priority-selection-test", "medium-2"))
	q.Enqueue(NewHighPriorityJob("priority-selection-test", "high-2"))

	q.StartDispatcher(1)
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 6 {
		t.Fatalf("Expected 6 jobs executed, got %d", len(executed))
	}

	// Verify high priority jobs come first
	highPriorityPositions := []int{}
	for i, payload := range executed {
		if payload == "high-1" || payload == "high-2" {
			highPriorityPositions = append(highPriorityPositions, i)
		}
	}

	if len(highPriorityPositions) != 2 {
		t.Errorf("Expected 2 high priority jobs, found %d", len(highPriorityPositions))
	}

	// High priority jobs should be in first positions
	for _, pos := range highPriorityPositions {
		if pos >= 2 {
			t.Errorf("High priority job executed at position %d, expected position 0 or 1", pos)
		}
	}
}

func TestSelectNextJobBehavior(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	// Test empty queues
	job, ok := q.selectNextJob()
	if ok {
		t.Error("Expected selectNextJob to return false for empty queues")
	}

	// Add jobs to different priority queues
	highJob := NewHighPriorityJob("test", "high")
	mediumJob := NewJob("test", "medium")
	lowJob := NewLowPriorityJob("test", "low")

	q.Enqueue(lowJob)
	q.Enqueue(mediumJob)
	q.Enqueue(highJob)

	// Should always get high priority first
	job, ok = q.selectNextJob()
	if !ok {
		t.Fatal("Expected to get a job")
	}
	if job.Priority != High {
		t.Errorf("Expected High priority job first, got %v", job.Priority)
	}

	// Should get medium priority next
	job, ok = q.selectNextJob()
	if !ok {
		t.Fatal("Expected to get a job")
	}
	if job.Priority != Medium {
		t.Errorf("Expected Medium priority job second, got %v", job.Priority)
	}

	// Should get low priority last
	job, ok = q.selectNextJob()
	if !ok {
		t.Fatal("Expected to get a job")
	}
	if job.Priority != Low {
		t.Errorf("Expected Low priority job third, got %v", job.Priority)
	}
}

func TestPriorityStarvationPrevention(t *testing.T) {
	q := NewJobQueue(50)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]Priority, 0)

	q.RegisterExecutor("starvation-test", func(job Job) error {
		mu.Lock()
		executed = append(executed, job.Priority)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return nil
	})

	// Add many low priority jobs
	for i := 0; i < 10; i++ {
		q.Enqueue(NewLowPriorityJob("starvation-test", i))
	}

	// Add medium and high priority jobs continuously
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(20 * time.Millisecond)
			q.Enqueue(NewHighPriorityJob("starvation-test", "high-"+string(rune(i))))
			q.Enqueue(NewJob("starvation-test", "medium-"+string(rune(i))))
		}
	}()

	q.StartDispatcher(2)
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Count executions by priority
	highCount := 0
	mediumCount := 0
	lowCount := 0

	for _, priority := range executed {
		switch priority {
		case High:
			highCount++
		case Medium:
			mediumCount++
		case Low:
			lowCount++
		}
	}

	// Low priority jobs should eventually execute (no complete starvation)
	if lowCount == 0 {
		t.Error("Low priority jobs were completely starved")
	}

	// High priority should execute more than low priority
	if highCount <= lowCount {
		t.Errorf("Expected more high priority executions (%d) than low priority (%d)", highCount, lowCount)
	}
}
