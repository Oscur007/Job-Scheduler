package main

import (
	"log"
	"net/http"
	"github.com/Oscur007/job-scheduler/internal/api"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/Oscur007/job-scheduler/internal/store"
	"github.com/go-chi/chi/v5"
)

func main() {
	q := queue.NewRedisQueue("localhost:6379")

	pgStore, err := store.NewPostgresStore("postgres://jobuser:jobpass@localhost:5432/jobscheduler?sslmode=disable")
	if err != nil {
		log.Fatalf("could not connect to postgres: %v", err)
	}
	defer pgStore.Close()

	h := api.NewHandler(pgStore, q)

	r := chi.NewRouter()
	r.Post("/jobs", h.CreateJob)
	r.Get("/jobs", h.ListJobs)
	r.Get("/jobs/{id}", h.GetJob)

	log.Println("server listening on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatal(err)
	}
}