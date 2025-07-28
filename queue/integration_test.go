package queue

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyWithPersistenceErrors(t *testing.T) {
	config := Config{
		BufferSize:        50,
		SchedulerInterval: 10 * time.Millisecond,
		MaxDelayedJobs:    100,
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	// Persistence that sometimes fails
	store := &MockPersistence{}
	q.SetPersistence(store)

	var successCount int64
	var failCount int64

	execConfig := ExecutorConfig{
		MaxConcurrentJobs: 3,
		Timeout:           100 * time.Millisecond,
	}

	q.RegisterExecutorWithConfig("integration-test", func(job Job) error {
		// Simulate work
		time.Sleep(20 * time.Millisecond)

		// Randomly fail some jobs
		if job.Payload.(int)%5 == 0 {
			atomic.AddInt64(&failCount, 1)
			return errors.New("simulated failure")
		}

		atomic.AddInt64(&successCount, 1)
		return nil
	}, execConfig)

	q.StartDispatcher(5)

	// Enqueue mix of immediate and delayed jobs
	for i := 0; i < 20; i++ {
		job := NewJob("integration-test", i)
		if i%3 == 0 {
			// Make some jobs delayed
			q.EnqueueDelayed(job, time.Duration(i*10)*time.Millisecond)
		} else {
			q.Enqueue(job)
		}

		// Introduce some persistence failures mid-way
		if i == 10 {
			store.shouldFailSave = true
		}
		if i == 15 {
			store.shouldFailSave = false
		}
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	totalSuccess := atomic.LoadInt64(&successCount)
	totalFail := atomic.LoadInt64(&failCount)

	t.Logf("Success: %d, Failures: %d", totalSuccess, totalFail)

	// Should have processed some jobs despite persistence issues
	if totalSuccess+totalFail == 0 {
		t.Error("No jobs were processed")
	}

	// Verify stats
	stats := q.GetStats()
	if stats.ExecutorStats["integration-test"].MaxJobs != 3 {
		t.Errorf("Expected max concurrent jobs to be 3, got %d",
			stats.ExecutorStats["integration-test"].MaxJobs)
	}
}

func TestHighLoadScenario(t *testing.T) {
	config := Config{
		BufferSize:        200,
		SchedulerInterval: 5 * time.Millisecond,
		MaxDelayedJobs:    500,
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	var processed int64
	var mu sync.Mutex
	priorities := make(map[Priority]int)

	// Different executors with different concurrency limits
	fastExecConfig := ExecutorConfig{
		MaxConcurrentJobs: 10,
		Timeout:           50 * time.Millisecond,
	}

	slowExecConfig := ExecutorConfig{
		MaxConcurrentJobs: 2,
		Timeout:           200 * time.Millisecond,
	}

	q.RegisterExecutorWithConfig("fast", func(job Job) error {
		atomic.AddInt64(&processed, 1)
		mu.Lock()
		priorities[job.Priority]++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	}, fastExecConfig)

	q.RegisterExecutorWithConfig("slow", func(job Job) error {
		atomic.AddInt64(&processed, 1)
		mu.Lock()
		priorities[job.Priority]++
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return nil
	}, slowExecConfig)

	q.StartDispatcher(8)

	// Generate high load
	go func() {
		for i := 0; i < 100; i++ {
			jobType := "fast"
			if i%5 == 0 {
				jobType = "slow"
			}

			var job Job
			switch i % 3 {
			case 0:
				job = NewHighPriorityJob(jobType, i)
			case 1:
				job = NewJob(jobType, i)
			case 2:
				job = NewLowPriorityJob(jobType, i)
			}

			if i%4 == 0 {
				q.EnqueueDelayed(job, time.Duration(i%50)*time.Millisecond)
			} else {
				q.Enqueue(job)
			}

			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Wait for processing
	time.Sleep(2 * time.Second)

	finalProcessed := atomic.LoadInt64(&processed)
	t.Logf("Processed %d jobs", finalProcessed)

	mu.Lock()
	highPriorityCount := priorities[High]
	mediumPriorityCount := priorities[Medium]
	lowPriorityCount := priorities[Low]
	mu.Unlock()

	t.Logf("Priority breakdown - High: %d, Medium: %d, Low: %d",
		highPriorityCount, mediumPriorityCount, lowPriorityCount)

	// Should process a significant number of jobs
	if finalProcessed < 50 {
		t.Errorf("Expected at least 50 jobs processed, got %d", finalProcessed)
	}

	// High priority jobs should be processed more than low priority
	if highPriorityCount < lowPriorityCount {
		t.Errorf("Expected more high priority jobs (%d) than low priority (%d)",
			highPriorityCount, lowPriorityCount)
	}

	// Verify final queue states
	stats := q.GetStats()
	t.Logf("Final stats: %+v", stats)

	// Queues should be mostly empty
	totalInQueues := stats.HighQueueSize + stats.MediumQueueSize + stats.LowQueueSize
	if totalInQueues > 10 {
		t.Logf("Warning: %d jobs still in queues", totalInQueues)
	}
}
