package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/muttaqinrizal/go-taskq/internal/domain"
)

// TaskHandler is a function type that processes a specific task.
type TaskHandler func(ctx context.Context, job *domain.Job) error

// Pool manages a pool of workers to process jobs from the queue.
type Pool struct {
	queue       domain.QueueRepository
	concurrency int
	handlers    map[domain.TaskType]TaskHandler
	wg          sync.WaitGroup
	quit        chan struct{}
}

// NewPool creates a new worker pool.
func NewPool(queue domain.QueueRepository, concurrency int) *Pool {
	return &Pool{
		queue:       queue,
		concurrency: concurrency,
		handlers:    make(map[domain.TaskType]TaskHandler),
		quit:        make(chan struct{}),
	}
}

// RegisterHandler registers a handler for a specific task type.
func (p *Pool) RegisterHandler(taskType domain.TaskType, handler TaskHandler) {
	p.handlers[taskType] = handler
}

// Start begins processing jobs from the queue using multiple workers.
func (p *Pool) Start(ctx context.Context) {
	log.Printf("Starting worker pool with %d workers\n", p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

// Stop gracefully shuts down the worker pool, waiting for active jobs to finish.
func (p *Pool) Stop() {
	log.Println("Stopping worker pool...")
	close(p.quit)
	p.wg.Wait()
	log.Println("Worker pool stopped gracefully")
}

func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	log.Printf("Worker %d started\n", id)

	for {
		select {
		case <-p.quit:
			log.Printf("Worker %d shutting down\n", id)
			return
		case <-ctx.Done():
			log.Printf("Worker %d context cancelled\n", id)
			return
		default:
			// Attempt to dequeue a job
			job, err := p.queue.Dequeue(ctx)
			if err != nil {
				log.Printf("Worker %d failed to dequeue: %v\n", id, err)
				time.Sleep(1 * time.Second) // Backoff
				continue
			}

			if job == nil {
				// Queue is empty, short sleep before trying again to avoid tight loop
				// (Though BRPop already blocks, this is a fallback if it returns nil immediately)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			p.processJob(ctx, id, job)
		}
	}
}

func (p *Pool) processJob(ctx context.Context, workerID int, job *domain.Job) {
	log.Printf("Worker %d processing job %s (type: %s)\n", workerID, job.ID, job.Type)

	// Mark as processing
	if err := p.queue.UpdateStatus(ctx, job.ID, domain.JobStatusProcessing, ""); err != nil {
		log.Printf("Worker %d failed to update status to processing for job %s: %v\n", workerID, job.ID, err)
	}

	handler, exists := p.handlers[job.Type]
	if !exists {
		errMsg := fmt.Sprintf("no handler registered for task type: %s", job.Type)
		log.Printf("Worker %d error: %s\n", workerID, errMsg)
		p.queue.UpdateStatus(ctx, job.ID, domain.JobStatusFailed, errMsg)
		return
	}

	// Execute the handler
	err := handler(ctx, job)
	if err != nil {
		log.Printf("Worker %d failed to process job %s: %v\n", workerID, job.ID, err)
		p.queue.UpdateStatus(ctx, job.ID, domain.JobStatusFailed, err.Error())
		return
	}

	// Mark as completed
	log.Printf("Worker %d successfully processed job %s\n", workerID, job.ID)
	p.queue.UpdateStatus(ctx, job.ID, domain.JobStatusCompleted, "")
}
