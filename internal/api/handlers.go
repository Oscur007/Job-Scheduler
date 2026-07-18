package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/Oscur007/job-scheduler/internal/store"
	"github.com/go-chi/chi/v5"
)

type Handler struct {
	Store *store.PostgresStore
	Queue *queue.RedisQueue
}

func NewHandler(s *store.PostgresStore, q *queue.RedisQueue) *Handler {
	return &Handler{Store: s, Queue: q}
}

type CreateJobRequest struct {
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	DelaySecs  int    `json:"delay_seconds"`
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}

	delay := time.Duration(req.DelaySecs) * time.Second
	j := job.New(req.Type, req.Payload, req.MaxRetries, req.Priority, delay)

	ctx := r.Context()
	if err := h.Store.InsertJob(ctx, j); err != nil {
		http.Error(w, "failed to persist job", http.StatusInternalServerError)
		return
	}

	jobJSON, err := j.Serialize()
	if err != nil {
		http.Error(w, "failed to serialize job", http.StatusInternalServerError)
		return
	}

	score := queue.ComputeScore(j.ScheduledAt, j.Priority)
	if err := h.Queue.EnqueueJob(ctx, j.ID, jobJSON, score); err != nil {
		http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(j)
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	j, err := h.Store.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.Store.ListJobs(r.Context())
	if err != nil {
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}