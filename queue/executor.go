package queue

type ExecutorFunc func(job Job) error

type ExecutorResult struct {
	Error error
}
