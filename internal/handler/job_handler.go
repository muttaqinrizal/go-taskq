package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/muttaqinrizal/go-taskq/internal/domain"
)

type JobHandler struct {
	queue domain.QueueRepository
}

func NewJobHandler(queue domain.QueueRepository) *JobHandler {
	return &JobHandler{queue: queue}
}

type EnqueueRequest struct {
	Type    domain.TaskType `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (h *JobHandler) EnqueueJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req EnqueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if req.Type == "" || len(req.Payload) == 0 {
		http.Error(w, "Type and payload are required", http.StatusBadRequest)
		return
	}

	job := domain.NewJob(req.Type, req.Payload)

	if err := h.queue.Enqueue(r.Context(), job); err != nil {
		http.Error(w, "Failed to enqueue job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Job enqueued successfully",
		"job_id":  job.ID,
	})
}
