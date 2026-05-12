package domain

import "context"

// QueueRepository defines the interface for interacting with the job queue.
type QueueRepository interface {
	// Enqueue adds a new job to the queue.
	Enqueue(ctx context.Context, job *Job) error

	// Dequeue retrieves and removes a job from the queue.
	// It should return nil, nil if the queue is empty.
	Dequeue(ctx context.Context) (*Job, error)

	// UpdateStatus updates the status of a job (e.g., to completed or failed).
	UpdateStatus(ctx context.Context, jobID string, status JobStatus, errMessage string) error
}
