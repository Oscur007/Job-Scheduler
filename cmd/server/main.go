package main

import (
	"context"
	"log"
	"net/http"

	"github.com/Oscur007/job-scheduler/internal/api"
	"github.com/Oscur007/job-scheduler/internal/config"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/Oscur007/job-scheduler/internal/store"
	"github.com/go-chi/chi/v5"
)

func main() {
	cfg := config.Load()

	q := queue.NewRedisQueue(cfg.RedisAddr)
	if err := q.Ping(context.Background()); err != nil {
		log.Fatalf("could not connect to redis: %v", err)
	}

	pgStore, err := store.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("could not connect to postgres: %v", err)
	}
	defer pgStore.Close()

	h := api.NewHandler(pgStore, q)

	r := chi.NewRouter()
	r.Post("/jobs", h.CreateJob)
	r.Get("/jobs", h.ListJobs)
	r.Get("/jobs/{id}", h.GetJob)
	r.Get("/stats", h.GetStats)

	r.Handle("/*", http.FileServer(http.Dir(cfg.UIDir)))

	log.Printf("server listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, r); err != nil {
		log.Fatal(err)
	}
}