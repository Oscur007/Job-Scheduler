package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/Oscur007/job-scheduler/internal/store"
)

func main() {
	ctx := context.Background()

	q := queue.NewRedisQueue("localhost:6379")
	if err := q.Ping(ctx); err != nil {
		log.Fatalf("could not connect to redis: %v", err)
	}

	pgStore, err := store.NewPostgresStore("postgres://jobuser:jobpass@localhost:5432/jobscheduler?sslmode=disable")
	if err != nil {
		log.Fatalf("could not connect to postgres: %v", err)
	}
	defer pgStore.Close()

	j := job.New("send_email", `{"to":"test@example.com"}`, 3)

	if err := pgStore.InsertJob(ctx, j); err != nil {
		log.Fatalf("failed to insert job into postgres: %v", err)
	}

	jobJSON, err := j.Serialize()
	if err != nil {
		log.Fatalf("failed to serialize job: %v", err)
	}

	score := float64(time.Now().Unix())
	if err := q.EnqueueJob(ctx, j.ID, jobJSON, score); err != nil {
		log.Fatalf("failed to enqueue job: %v", err)
	}

	fmt.Printf("enqueued job: %s\n", j.ID)
}