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

	q := tinymq.NewQueue(100)
	defer q.Stop()

	q.RegisterExecutor("print", func(job tinymq.Job) {
		fmt.Printf("[EXECUTOR] Job %s at %v: %v\n", job.ID, time.Now().Format("15:04:05.000"), job.Payload)
	})

	q.StartDispatcher(4)

	fmt.Println("Enqueueing immediate jobs...")
	for i := 0; i < 3; i++ {
		job := tinymq.NewJob("print", fmt.Sprintf("Immediate job %d", i))
		q.Enqueue(job)
	}

	fmt.Println("Enqueueing delayed jobs...")
	for i := 0; i < 3; i++ {
		delay := time.Duration(i+1) * time.Second
		job := tinymq.NewDelayedJob("print", fmt.Sprintf("Delayed job %d (delay: %v)", i, delay), delay)
		q.Enqueue(job)
	}

	fmt.Println("Enqueueing jobs with EnqueueDelayed...")
	for i := 0; i < 3; i++ {
		delay := time.Duration(i*500) * time.Millisecond
		job := tinymq.NewJob("print", fmt.Sprintf("EnqueueDelayed job %d (delay: %v)", i, delay))
		q.EnqueueDelayed(job, delay)
	}

	time.Sleep(5 * time.Second)
	fmt.Println("Done!")
}
