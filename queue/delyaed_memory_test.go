package queue

import (
	"testing"
	"time"
)

func TestDelayedJobsMemoryLimit(t *testing.T) {
	config := Config{
		BufferSize:        10,
		SchedulerInterval: 100 * time.Millisecond,
		MaxDelayedJobs:    5, // Small limit for testing
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	// Add more delayed jobs than the limit
	for i := 0; i < 8; i++ {
		job := NewJob("test", i)
		q.EnqueueDelayed(job, time.Hour) // Long delay so they stay in queue
	}

	time.Sleep(50 * time.Millisecond) // Let processing settle

	q.delayedMu.Lock()
	actualCount := q.delayedJobs.Len()
	q.delayedMu.Unlock()

	if actualCount != 5 {
		t.Errorf("Expected delayed jobs count to be capped at 5, got %d", actualCount)
	}
}

func TestDelayedJobsWarningThreshold(t *testing.T) {
	config := Config{
		BufferSize:        10,
		SchedulerInterval: 100 * time.Millisecond,
		MaxDelayedJobs:    10,
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	// Add jobs up to warning threshold (90% of 10 = 9)
	for i := 0; i < 9; i++ {
		job := NewJob("test", i)
		q.EnqueueDelayed(job, time.Hour)
	}

	time.Sleep(50 * time.Millisecond)

	q.delayedMu.Lock()
	actualCount := q.delayedJobs.Len()
	q.delayedMu.Unlock()

	if actualCount != 9 {
		t.Errorf("Expected 9 delayed jobs, got %d", actualCount)
	}
}

func TestDelayedJobsDropsOldest(t *testing.T) {
	config := Config{
		BufferSize:        10,
		SchedulerInterval: 100 * time.Millisecond,
		MaxDelayedJobs:    3,
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	now := time.Now()

	// Add jobs with increasing delays (so we can verify oldest is dropped)
	job1 := NewJob("test", "first")
	job1.RunAt = now.Add(1 * time.Hour)
	q.enqueueDelayed(job1)

	job2 := NewJob("test", "second")
	job2.RunAt = now.Add(2 * time.Hour)
	q.enqueueDelayed(job2)

	job3 := NewJob("test", "third")
	job3.RunAt = now.Add(3 * time.Hour)
	q.enqueueDelayed(job3)

	// This should cause the first job to be dropped
	job4 := NewJob("test", "fourth")
	job4.RunAt = now.Add(4 * time.Hour)
	q.enqueueDelayed(job4)

	time.Sleep(50 * time.Millisecond)

	q.delayedMu.Lock()
	count := q.delayedJobs.Len()

	// Verify we still have 3 jobs and the first one was dropped
	found_first := false
	for i := 0; i < count; i++ {
		if q.delayedJobs[i].job.Payload == "first" {
			found_first = true
			break
		}
	}
	q.delayedMu.Unlock()

	if count != 3 {
		t.Errorf("Expected 3 delayed jobs after overflow, got %d", count)
	}

	if found_first {
		t.Error("Expected oldest job (first) to be dropped, but it was still found")
	}
}

func TestUnlimitedDelayedJobs(t *testing.T) {
	config := Config{
		BufferSize:        10,
		SchedulerInterval: 100 * time.Millisecond,
		MaxDelayedJobs:    0, // 0 means unlimited
	}

	q := NewJobQueueWithConfig(config)
	defer q.Stop()

	// Add many jobs - should not be limited
	for i := 0; i < 100; i++ {
		job := NewJob("test", i)
		q.EnqueueDelayed(job, time.Hour)
	}

	time.Sleep(50 * time.Millisecond)

	q.delayedMu.Lock()
	actualCount := q.delayedJobs.Len()
	q.delayedMu.Unlock()

	if actualCount != 100 {
		t.Errorf("Expected 100 delayed jobs with unlimited config, got %d", actualCount)
	}
}
