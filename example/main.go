package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oukrims/tinymq"
)

func main() {
	logger := log.New(os.Stdout, "[tinymq] ", log.LstdFlags)
	tinymq.SetLogger(logger)

	// Example with custom scheduler interval for time-sensitive jobs
	config := tinymq.Config{
		BufferSize:        100,
		SchedulerInterval: 50 * time.Millisecond, // More responsive than default 100ms
	}
	q := tinymq.NewQueueWithConfig(config)
	defer q.Stop()

	// Alternative: use default config
	// q := tinymq.NewQueue(100)

	q.RegisterExecutor("process", func(job tinymq.Job) error {
		fmt.Printf("[EXEC] %v Priority - Job %s: %v\n",
			job.Priority, job.ID[:8], job.Payload)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	q.StartDispatcher(2)

	fmt.Println("=== Enqueueing mixed priority jobs ===")

	jobs := []struct {
		priority string
		job      tinymq.Job
	}{
		{"Low", tinymq.NewLowPriorityJob("process", "Background cleanup")},
		{"High", tinymq.NewHighPriorityJob("process", "Critical alert!")},
		{"Medium", tinymq.NewJob("process", "Regular processing")},
		{"Low", tinymq.NewLowPriorityJob("process", "Archive old data")},
		{"High", tinymq.NewHighPriorityJob("process", "Emergency fix!")},
		{"Medium", tinymq.NewJob("process", "User notification")},
		{"Low", tinymq.NewLowPriorityJob("process", "Generate reports")},
	}

	for _, j := range jobs {
		fmt.Printf("Enqueuing %s priority: %v\n", j.priority, j.job.Payload)
		q.Enqueue(j.job)
	}

	fmt.Println("\n=== Priority with delayed jobs (faster scheduling) ===")

	delayedJobs := []struct {
		priority string
		job      tinymq.Job
		delay    time.Duration
	}{
		{"Low", tinymq.NewLowPriorityJob("process", "Delayed low priority"), 200 * time.Millisecond},
		{"High", tinymq.NewHighPriorityJob("process", "Delayed high priority"), 200 * time.Millisecond},
		{"Medium", tinymq.NewJob("process", "Delayed medium priority"), 200 * time.Millisecond},
	}

	for _, j := range delayedJobs {
		fmt.Printf("Scheduling %s priority in %v: %v\n", j.priority, j.delay, j.job.Payload)
		q.EnqueueDelayed(j.job, j.delay)
	}

	fmt.Println("\n=== Chaining priority methods ===")

	chainedJob := tinymq.NewJob("process", "Chained job")
	chainedJobPtr := &chainedJob

	chainedJobPtr.WithHighPriority().WithMaxRetries(5)
	q.Enqueue(*chainedJobPtr)

	fmt.Println("\nWatching execution order (High → Medium → Low)...")
	time.Sleep(2 * time.Second)
	fmt.Println("Done!")
}
