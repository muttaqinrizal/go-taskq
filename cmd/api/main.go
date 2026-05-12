package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/muttaqinrizal/go-taskq/config"
	"github.com/muttaqinrizal/go-taskq/internal/domain"
	"github.com/muttaqinrizal/go-taskq/internal/handler"
	"github.com/muttaqinrizal/go-taskq/internal/queue"
	"github.com/muttaqinrizal/go-taskq/internal/worker"
)

func main() {
	log.Println("Starting Task Queue Service...")

	// 1. Setup Redis
	redisClient, err := config.SetupRedis()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// 2. Setup Queue Repository
	redisQueue := queue.NewRedisQueue(redisClient)

	// 3. Setup Worker Pool
	workerConcurrency := 5 // Number of concurrent workers
	pool := worker.NewPool(redisQueue, workerConcurrency)

	// Register Task Handlers
	pool.RegisterHandler(domain.TaskTypeSendEmail, worker.HandleSendEmail)
	pool.RegisterHandler(domain.TaskTypeResizeImg, worker.HandleResizeImage)

	// Start Worker Pool in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	// 4. Setup HTTP Handlers
	jobHandler := handler.NewJobHandler(redisQueue)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/jobs", jobHandler.EnqueueJob)

	// 5. Setup HTTP Server
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start HTTP Server in the background
	go func() {
		log.Println("HTTP server is running on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on :8080: %v\n", err)
		}
	}()

	// 6. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Stop HTTP server
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Stop workers
	cancel() // Cancel the context passed to workers
	pool.Stop() // Wait for workers to finish current jobs

	log.Println("Server exiting")
}
