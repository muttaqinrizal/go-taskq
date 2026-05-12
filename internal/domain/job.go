package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// TaskType represents the kind of task to be executed.
type TaskType string

const (
	TaskTypeSendEmail TaskType = "send_email"
	TaskTypeResizeImg TaskType = "resize_image"
)

// Job represents a single unit of work in the queue.
type Job struct {
	ID        string          `json:"id"`
	Type      TaskType        `json:"type"`
	Payload   json.RawMessage `json:"payload"` // Flexible payload depending on TaskType
	Status    JobStatus       `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Error     string          `json:"error,omitempty"`
}

// JobStatus represents the current state of a job.
type JobStatus string

const (
	JobStatusPending    JobStatus = "pending"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

// NewJob creates a new Job instance.
func NewJob(taskType TaskType, payload []byte) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:        uuid.New().String(),
		Type:      taskType,
		Payload:   payload,
		Status:    JobStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
