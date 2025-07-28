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
