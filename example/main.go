package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/oukrims/tinymq"
)

func main() {
	logger := log.New(os.Stdout, "[tinymq] ", log.LstdFlags)
	tinymq.SetLogger(logger)

	q := tinymq.NewQueue(100)
	defer q.Stop()

	q.RegisterExecutor("reliable", func(job tinymq.Job) error {
		fmt.Printf("[RELIABLE] Processing job %s: %v\n", job.ID, job.Payload)
		return nil
	})

	q.RegisterExecutor("flaky", func(job tinymq.Job) error {
		fmt.Printf("[FLAKY] Attempt %d for job %s: %v\n", job.Retries+1, job.ID, job.Payload)

		if rand.Float32() < 0.7 { // 70% failure rate
			return errors.New("random failure occurred")
		}

		fmt.Printf("[FLAKY] Success on attempt %d!\n", job.Retries+1)
		return nil
	})

	q.RegisterExecutor("always-fails", func(job tinymq.Job) error {
		fmt.Printf("[FAIL] Attempt %d for job %s: %v\n", job.Retries+1, job.ID, job.Payload)
		return errors.New("this job always fails")
	})

	q.StartDispatcher(2)

	// Reliable job
	job1 := tinymq.NewJob("reliable", "This will work")
	q.Enqueue(job1)

	// Flaky job with default retry config
	job2 := tinymq.NewJob("flaky", "This might work after retries")
	q.Enqueue(job2)

	// Flaky job with custom retry config
	job3 := tinymq.NewJob("flaky", "Custom retry config")
	job3.WithRetryConfig(tinymq.RetryConfig{
		MaxRetries:      5,
		InitialDelay:    200 * time.Millisecond,
		MaxDelay:        2 * time.Second,
		BackoffMultiple: 1.5,
	})
	q.Enqueue(job3)

	// Job that will exhaust retries
	job4 := tinymq.NewJob("always-fails", "This will fail permanently")
	job4.WithMaxRetries(2)
	q.Enqueue(job4)

	fmt.Println("Jobs enqueued. Watching execution...")
	time.Sleep(15 * time.Second)
	fmt.Println("Done!")
}
