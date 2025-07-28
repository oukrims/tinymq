package queue

import (
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkWorkerPrioritySelection(b *testing.B) {
	q := NewJobQueue(1000)
	defer q.Stop()

	var executed int64
	q.RegisterExecutor("benchmark", func(job Job) error {
		atomic.AddInt64(&executed, 1)
		return nil
	})

	q.StartDispatcher(4)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			var job Job
			switch i % 3 {
			case 0:
				job = NewHighPriorityJob("benchmark", i)
			case 1:
				job = NewJob("benchmark", i)
			case 2:
				job = NewLowPriorityJob("benchmark", i)
			}
			q.Enqueue(job)
			i++
		}
	})

	// Wait for jobs to complete
	for atomic.LoadInt64(&executed) < int64(b.N) {
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkSelectNextJob(b *testing.B) {
	q := NewJobQueue(1000)
	defer q.Stop()

	// Pre-fill queues with jobs
	for i := 0; i < 100; i++ {
		q.Enqueue(NewHighPriorityJob("test", i))
		q.Enqueue(NewJob("test", i))
		q.Enqueue(NewLowPriorityJob("test", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := q.selectNextJob()
		if !ok {
			// Refill queues if empty
			q.Enqueue(NewHighPriorityJob("test", i))
			q.Enqueue(NewJob("test", i))
			q.Enqueue(NewLowPriorityJob("test", i))
		}
	}
}

func BenchmarkPrioritySelectionVsNested(b *testing.B) {
	q := NewJobQueue(1000)
	defer q.Stop()

	// Fill with mixed priority jobs
	for i := 0; i < 333; i++ {
		q.Enqueue(NewHighPriorityJob("test", i))
		q.Enqueue(NewJob("test", i))
		q.Enqueue(NewLowPriorityJob("test", i))
	}

	b.Run("NewMethod", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.selectNextJob()
		}
	})
}
