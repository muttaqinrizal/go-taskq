package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/muttaqinrizal/go-taskq/internal/domain"
	"github.com/redis/go-redis/v9"
)

const queueKey = "taskq:queue:default"
const jobKeyPrefix = "taskq:job:"

type RedisQueue struct {
	client *redis.Client
}

// NewRedisQueue creates a new instance of RedisQueue.
func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{
		client: client,
	}
}

// Enqueue adds a job to the Redis list and stores its metadata.
func (q *RedisQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Use pipeline to ensure atomic operation
	pipe := q.client.TxPipeline()

	// Store job data
	pipe.Set(ctx, jobKeyPrefix+job.ID, jobJSON, 0)

	// Push job ID to the queue (list)
	pipe.LPush(ctx, queueKey, job.ID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	return nil
}

// Dequeue pops a job from the Redis list.
func (q *RedisQueue) Dequeue(ctx context.Context) (*domain.Job, error) {
	// BRPOP blocks until a job is available or timeout occurs
	// Using 0 timeout means it will block indefinitely until context is cancelled
	result, err := q.client.BRPop(ctx, 0, queueKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
			return nil, nil // Queue is empty or context cancelled
		}
		return nil, fmt.Errorf("failed to pop from queue: %w", err)
	}

	if len(result) < 2 {
		return nil, nil
	}

	jobID := result[1]
	jobKey := jobKeyPrefix + jobID

	// Fetch job details
	jobJSON, err := q.client.Get(ctx, jobKey).Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get job details: %w", err)
	}

	var job domain.Job
	if err := json.Unmarshal(jobJSON, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}

// UpdateStatus updates the job's status and error message in Redis.
func (q *RedisQueue) UpdateStatus(ctx context.Context, jobID string, status domain.JobStatus, errMessage string) error {
	jobKey := jobKeyPrefix + jobID

	// Fetch existing job
	jobJSON, err := q.client.Get(ctx, jobKey).Bytes()
	if err != nil {
		return fmt.Errorf("failed to get job for update: %w", err)
	}

	var job domain.Job
	if err := json.Unmarshal(jobJSON, &job); err != nil {
		return fmt.Errorf("failed to unmarshal job for update: %w", err)
	}

	// Update fields
	job.Status = status
	job.Error = errMessage

	updatedJSON, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal updated job: %w", err)
	}

	// Save back to Redis
	if err := q.client.Set(ctx, jobKey, updatedJSON, 0).Err(); err != nil {
		return fmt.Errorf("failed to save updated job: %w", err)
	}

	return nil
}
