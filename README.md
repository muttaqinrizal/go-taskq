# Distributed Task Queue (Golang)

This is a high-performance distributed task queue / job scheduler built with Golang and Redis

## Features
- **Clean Architecture:** Domain, Delivery, and Infrastructure layers.
- **Concurrency:** Uses Goroutines and Channels in a Worker Pool pattern to process multiple jobs simultaneously.
- **Redis Queue:** Utilizes Redis Lists and Pipelines for atomic operations and high-throughput job queuing.
- **Graceful Shutdown:** Ensures workers finish current jobs before exiting.

## Architecture

1. **Domain:** Contains the business logic, `Job`, `TaskType`, and repository interfaces.
2. **Infrastructure (Queue):** Redis implementation of the queue.
3. **Core Engine (Worker):** The worker pool that dequeues and processes jobs concurrently.
4. **Delivery (Handler):** HTTP API to enqueue jobs.

## Running Locally

### Prerequisites
- Go 1.21+
- Redis (running on `localhost:6379` or via Docker)

### Start Redis (via Docker)
```bash
docker run -d -p 6379:6379 redis:latest
```

### Start the Application
```bash
go run cmd/api/main.go
```

## API Usage

### Enqueue a Task (Send Email)
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome!",
      "body": "Hello world"
    }
  }'
```

### Enqueue a Task (Resize Image)
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "resize_image",
    "payload": {
      "image_url": "https://example.com/image.png",
      "width": 800,
      "height": 600
    }
  }'
```

Watch the terminal output to see the workers picking up and processing the jobs concurrently!
