package api

import (
	"encoding/json"
	"net/http"
	"time"
	"strconv"

	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/Oscur007/job-scheduler/internal/store"
	"github.com/go-chi/chi/v5"
)

var allowedPageSizes = map[int]bool{10: true, 25: true, 50: true, 100: true}

type Handler struct {
	Store *store.PostgresStore
	Queue *queue.RedisQueue
}

type ListJobsResponse struct {
	Jobs     []*job.Job `json:"jobs"`
	Total    int        `json:"total"`
	Page     int        `json:"page"`
	PageSize int        `json:"page_size"`
}

type CreateJobRequest struct {
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	DelaySecs  int    `json:"delay_seconds"`
}

func NewHandler(s *store.PostgresStore, q *queue.RedisQueue) *Handler {
	return &Handler{Store: s, Queue: q}
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
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if !allowedPageSizes[pageSize] {
		pageSize = 10
	}

	ctx := r.Context()
	total, err := h.Store.CountJobs(ctx)
	if err != nil {
		http.Error(w, "failed to count jobs", http.StatusInternalServerError)
		return
	}

	jobs, err := h.Store.ListJobs(ctx, pageSize, (page-1)*pageSize)
	if err != nil {
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ListJobsResponse{
		Jobs:     jobs,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	counts, err := h.Store.StatusCounts(r.Context())
	if err != nil {
		http.Error(w, "failed to get stats", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(counts)
}