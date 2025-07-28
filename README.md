# TinyMQ

A lightweight, high-performance in-memory job queue for Go with priority scheduling, delayed job execution, and configurable concurrency controls.

## Features

* **Priority-based job scheduling** (High, Medium, Low)
* **Delayed job execution** with configurable scheduler intervals
* **Configurable concurrency limits** per executor
* **Retry mechanism** with exponential backoff
* **Memory-safe delayed job limits** to prevent memory leaks
* **Pluggable persistence** for job durability
* **Comprehensive stats and monitoring**
* **Timeout handling** for job execution
* **Thread-safe operations**

## Installation

```bash
go get github.com/oukrims/tinymq
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/oukrims/tinymq"
)

func main() {
    // Create a new queue
    q := tinymq.NewQueue(100)
    defer q.Stop()

    // Register an executor
    q.RegisterExecutor("email", func(job tinymq.Job) error {
        fmt.Printf("Sending email: %v\n", job.Payload)
        return nil
    })

    // Start workers
    q.StartDispatcher(3)

    // Enqueue jobs
    q.Enqueue(tinymq.NewJob("email", "Welcome email"))
    q.Enqueue(tinymq.NewHighPriorityJob("email", "Password reset"))
  
    time.Sleep(time.Second)
}
```

## Priority System

TinyMQ supports three priority levels that ensure critical jobs are processed first:

```go
// High priority - processed first
highJob := tinymq.NewHighPriorityJob("urgent", "Critical alert")

// Medium priority - default
mediumJob := tinymq.NewJob("normal", "Regular task")

// Low priority - processed last
lowJob := tinymq.NewLowPriorityJob("background", "Cleanup task")

// Chain priority methods
job := tinymq.NewJob("process", "data")
job.WithHighPriority().WithMaxRetries(5)
```

## Delayed Jobs

Schedule jobs to run at a specific time:

```go
// Run in 5 minutes
q.EnqueueDelayed(job, 5*time.Minute)

// Or set specific run time
job.RunAt = time.Now().Add(time.Hour)
q.Enqueue(job)
```

## Configuration

### Queue Configuration

```go
config := tinymq.Config{
    BufferSize:        200,                    // Queue buffer size
    SchedulerInterval: 50 * time.Millisecond, // Delayed job check interval
    MaxDelayedJobs:    1000,                  // Memory limit for delayed jobs
}
q := tinymq.NewQueueWithConfig(config)
```

### Executor Configuration

```go
execConfig := tinymq.ExecutorConfig{
    MaxConcurrentJobs: 5,               // Limit concurrent executions
    Timeout:           30 * time.Second, // Job execution timeout
}

q.RegisterExecutorWithConfig("processor", func(job tinymq.Job) error {
    // Your job logic here
    return nil
}, execConfig)
```

## Retry Configuration

Configure retry behavior for failed jobs:

```go
retryConfig := tinymq.RetryConfig{
    MaxRetries:      5,
    InitialDelay:    time.Second,
    MaxDelay:        5 * time.Minute,
    BackoffMultiple: 2.0,
}

job := tinymq.NewJob("flaky-task", "data")
job.WithRetryConfig(retryConfig)
```

## Persistence

Add persistence for job durability:

```go
type MyPersistence struct{}

func (p *MyPersistence) Save(job tinymq.Job) error {
    // Save job to database/file
    return nil
}

func (p *MyPersistence) Delete(jobID string) error {
    // Remove completed job
    return nil
}

func (p *MyPersistence) LoadAll() ([]tinymq.Job, error) {
    // Load jobs on startup
    return jobs, nil
}

q.SetPersistence(&MyPersistence{})
```

## Monitoring

Get queue statistics:

```go
stats := q.GetStats()
fmt.Printf("High priority jobs: %d\n", stats.HighQueueSize)
fmt.Printf("Delayed jobs: %d/%d\n", stats.DelayedJobsCount, stats.MaxDelayedJobs)

for jobType, execStats := range stats.ExecutorStats {
    fmt.Printf("Executor %s: %d/%d active\n", 
        jobType, execStats.ActiveJobs, execStats.MaxJobs)
}
```

## Advanced Usage

### Job Tagging

```go
job := tinymq.NewJob("process", "data")
job.Tags["user"] = "123"
job.Tags["priority"] = "urgent"
```

### Memory Management

Prevent memory issues with delayed jobs:

```go
config := tinymq.Config{
    MaxDelayedJobs: 10000, // Limit delayed jobs
}
// When limit is reached, oldest jobs are dropped
```

### Concurrency Control

```go
// Limit concurrent executions per job type
execConfig := tinymq.ExecutorConfig{
    MaxConcurrentJobs: 3, // Only 3 jobs of this type run simultaneously
    Timeout:           5 * time.Second,
}
```

## Error Handling

TinyMQ provides comprehensive error handling:

* **Execution timeouts** : Jobs that exceed their timeout are cancelled
* **Retry with backoff** : Failed jobs are retried with exponential backoff
* **Persistence errors** : Gracefully handled with logging
* **Queue overflow** : Jobs are dropped when queues are full (with logging)

## Performance

TinyMQ is designed for high performance:

* Lock-free job selection using Go channels
* Efficient priority scheduling
* Configurable concurrency limits
* Memory-efficient delayed job storage
* Minimal overhead for job execution

## Best Practices

1. **Set appropriate buffer sizes** based on your workload
2. **Configure concurrency limits** to prevent resource exhaustion
3. **Use priority levels wisely** - not everything needs high priority
4. **Set realistic timeouts** for job execution
5. **Monitor queue stats** to identify bottlenecks
6. **Implement persistence** for critical jobs
7. **Set delayed job limits** to prevent memory leaks

## Contributing

Contributions are welcome! Please ensure:

* Tests pass: `go test ./...`
* Code is formatted: `go fmt ./...`
* Add tests for new features
* Update documentation

## License

MIT License - see LICENSE file for details.

## Examples

See the `example/` directory for more comprehensive examples including:

* Priority job processing
* Delayed job scheduling
* Concurrency limiting
* Error handling and retries
* Performance monitoring
