package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/muttaqinrizal/go-taskq/internal/domain"
)

// MockQueue is a simple mock for domain.QueueRepository
type MockQueue struct {
	jobs          chan *domain.Job
	updatedStatus map[string]domain.JobStatus
}

func NewMockQueue() *MockQueue {
	return &MockQueue{
		jobs:          make(chan *domain.Job, 10),
		updatedStatus: make(map[string]domain.JobStatus),
	}
}

func (m *MockQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	m.jobs <- job
	return nil
}

func (m *MockQueue) Dequeue(ctx context.Context) (*domain.Job, error) {
	select {
	case job := <-m.jobs:
		return job, nil
	case <-ctx.Done():
		return nil, context.Canceled
	default:
		return nil, nil // Empty queue
	}
}

func (m *MockQueue) UpdateStatus(ctx context.Context, jobID string, status domain.JobStatus, errMessage string) error {
	m.updatedStatus[jobID] = status
	return nil
}

func TestWorkerPool_ProcessJob(t *testing.T) {
	mockQueue := NewMockQueue()
	pool := NewPool(mockQueue, 1)

	// Register a mock handler
	handled := false
	pool.RegisterHandler("test_task", func(ctx context.Context, job *domain.Job) error {
		handled = true
		return nil
	})

	job := domain.NewJob("test_task", []byte(`{}`))
	mockQueue.Enqueue(context.Background(), job)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	cancel()
	pool.Stop()

	if !handled {
		t.Errorf("Expected handler to be called")
	}

	if mockQueue.updatedStatus[job.ID] != domain.JobStatusCompleted {
		t.Errorf("Expected job status to be completed, got %s", mockQueue.updatedStatus[job.ID])
	}
}

func TestWorkerPool_ProcessJob_Fails(t *testing.T) {
	mockQueue := NewMockQueue()
	pool := NewPool(mockQueue, 1)

	// Register a mock handler that fails
	pool.RegisterHandler("test_task_fail", func(ctx context.Context, job *domain.Job) error {
		return errors.New("simulated error")
	})

	job := domain.NewJob("test_task_fail", []byte(`{}`))
	mockQueue.Enqueue(context.Background(), job)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	cancel()
	pool.Stop()

	if mockQueue.updatedStatus[job.ID] != domain.JobStatusFailed {
		t.Errorf("Expected job status to be failed, got %s", mockQueue.updatedStatus[job.ID])
	}
}
