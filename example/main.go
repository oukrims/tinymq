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

	q.RegisterExecutor("print", func(job tinymq.Job) {
		fmt.Printf("[EXECUTOR] Job %s: %v\n", job.ID, job.Payload)
	})

	q.StartDispatcher(4)

	for i := 0; i < 10; i++ {
		job := tinymq.NewJob("print", fmt.Sprintf("Hello from job %d", i))
		q.Enqueue(job)
	}

	time.Sleep(2 * time.Second)
}
