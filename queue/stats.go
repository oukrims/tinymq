package queue

type QueueStats struct {
	HighQueueSize    int
	MediumQueueSize  int
	LowQueueSize     int
	DelayedJobsCount int
	MaxDelayedJobs   int
	WorkerCount      int
}

func (jq *JobQueue) GetStats() QueueStats {
	jq.delayedMu.Lock()
	delayedCount := jq.delayedJobs.Len()
	jq.delayedMu.Unlock()

	return QueueStats{
		HighQueueSize:    len(jq.highQueue),
		MediumQueueSize:  len(jq.mediumQueue),
		LowQueueSize:     len(jq.lowQueue),
		DelayedJobsCount: delayedCount,
		MaxDelayedJobs:   jq.maxDelayedJobs,
		WorkerCount:      jq.workers,
	}
}
