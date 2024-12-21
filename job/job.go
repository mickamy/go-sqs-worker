package job

import (
	"context"
)

// Job is an interface for job execution
type Job interface {
	// Execute is a method to execute a job
	Execute(ctx context.Context, payloadStr string) error
}

// GetFunc is a function to get a job by job type
type GetFunc func(jobType string) (Job, error)
