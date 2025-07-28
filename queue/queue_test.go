package queue

import (
	"sync"
	"testing"
	"time"
)

type mockStore struct {
	sync.Mutex
	saved   []Job
	deleted []string
}

func (m *mockStore) Save(job Job) error {
	m.Lock()
	defer m.Unlock()
	m.saved = append(m.saved, job)
	return nil
}

func (m *mockStore) Delete(jobID string) error {
	m.Lock()
	defer m.Unlock()
	m.deleted = append(m.deleted, jobID)
	return nil
}

func (m *mockStore) LoadAll() ([]Job, error) {
	return m.saved, nil
}

func TestQueueExecution(t *testing.T) {
	store := &mockStore{}
	q := NewJobQueue(10)
	q.SetPersistence(store)

	done := make(chan bool)
	q.RegisterExecutor("test", func(job Job) {
		if job.Payload != "data" {
			t.Errorf("unexpected payload: %v", job.Payload)
		}
		done <- true
	})

	q.StartDispatcher(2)
	job := NewJob("test", "data")
	q.Enqueue(job)

	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for job execution")
	}

	if len(store.saved) != 1 {
		t.Errorf("expected 1 saved job, got %d", len(store.saved))
	}
	if len(store.deleted) != 1 {
		t.Errorf("expected 1 deleted job, got %d", len(store.deleted))
	}
}

func TestDelayedJobExecution(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	executed := make(chan time.Time, 1)
	q.RegisterExecutor("delayed", func(job Job) {
		executed <- time.Now()
	})

	q.StartDispatcher(1)

	delay := 200 * time.Millisecond
	startTime := time.Now()

	job := NewJob("delayed", "test")
	q.EnqueueDelayed(job, delay)

	select {
	case execTime := <-executed:
		actualDelay := execTime.Sub(startTime)
		if actualDelay < delay {
			t.Errorf("Job executed too early: expected >= %v, got %v", delay, actualDelay)
		}
		if actualDelay > delay+100*time.Millisecond {
			t.Errorf("Job executed too late: expected ~%v, got %v", delay, actualDelay)
		}
	case <-time.After(delay + 500*time.Millisecond):
		t.Fatal("Delayed job was never executed")
	}
}

func TestImmediateJobExecution(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	executed := make(chan bool, 1)
	q.RegisterExecutor("immediate", func(job Job) {
		executed <- true
	})

	q.StartDispatcher(1)

	job := NewJob("immediate", "test")
	q.Enqueue(job)

	select {
	case <-executed:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Immediate job was not executed quickly")
	}
}

func TestMultipleDelayedJobs(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	var mu sync.Mutex
	executed := make([]string, 0)

	q.RegisterExecutor("ordered", func(job Job) {
		mu.Lock()
		executed = append(executed, job.Payload.(string))
		mu.Unlock()
	})

	q.StartDispatcher(2)

	job1 := NewJob("ordered", "first")
	job2 := NewJob("ordered", "second")
	job3 := NewJob("ordered", "third")

	q.EnqueueDelayed(job3, 300*time.Millisecond)
	q.EnqueueDelayed(job1, 100*time.Millisecond)
	q.EnqueueDelayed(job2, 200*time.Millisecond)

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executed) != 3 {
		t.Fatalf("Expected 3 jobs executed, got %d", len(executed))
	}

	expected := []string{"first", "second", "third"}
	for i, exp := range expected {
		if executed[i] != exp {
			t.Errorf("Job %d: expected %s, got %s", i, exp, executed[i])
		}
	}
}

func TestJobRunAtRespected(t *testing.T) {
	q := NewJobQueue(10)
	defer q.Stop()

	executed := make(chan time.Time, 1)
	q.RegisterExecutor("timed", func(job Job) {
		executed <- time.Now()
	})

	q.StartDispatcher(1)

	futureTime := time.Now().Add(150 * time.Millisecond)
	job := NewJob("timed", "test")
	job.RunAt = futureTime
	q.Enqueue(job)

	select {
	case execTime := <-executed:
		if execTime.Before(futureTime.Add(-10 * time.Millisecond)) {
			t.Errorf("Job executed before RunAt time: expected >= %v, got %v", futureTime, execTime)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Job with future RunAt was never executed")
	}
}
