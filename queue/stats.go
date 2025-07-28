package queue

type ExecutorStats struct {
	ActiveJobs int
	MaxJobs    int
}

type QueueStats struct {
	HighQueueSize    int
	MediumQueueSize  int
	LowQueueSize     int
	DelayedJobsCount int
	MaxDelayedJobs   int
	WorkerCount      int
	ExecutorStats    map[string]ExecutorStats
}

func (jq *JobQueue) GetStats() QueueStats {
	jq.delayedMu.Lock()
	delayedCount := jq.delayedJobs.Len()
	jq.delayedMu.Unlock()

	jq.mu.RLock()
	executorStats := make(map[string]ExecutorStats)
	for jobType, execInfo := range jq.executors {
		executorStats[jobType] = ExecutorStats{
			ActiveJobs: execInfo.config.MaxConcurrentJobs - len(execInfo.semaphore),
			MaxJobs:    execInfo.config.MaxConcurrentJobs,
		}
	}
	jq.mu.RUnlock()

	return QueueStats{
		HighQueueSize:    len(jq.highQueue),
		MediumQueueSize:  len(jq.mediumQueue),
		LowQueueSize:     len(jq.lowQueue),
		DelayedJobsCount: delayedCount,
		MaxDelayedJobs:   jq.maxDelayedJobs,
		WorkerCount:      jq.workers,
		ExecutorStats:    executorStats,
	}
}
