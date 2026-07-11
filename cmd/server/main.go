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

	j1 := job.New("send_email", `{"to":"urgent@example.com"}`, 3, 10, 0)

	j2 := job.New("generate_report", `{"report":"monthly"}`, 3, 0, 10*time.Second)

	for _, j := range []*job.Job{j1, j2} {
		if err := pgStore.InsertJob(ctx, j); err != nil {
			log.Fatalf("failed to insert job into postgres: %v", err)
		}

		jobJSON, err := j.Serialize()
		if err != nil {
			log.Fatalf("failed to serialize job: %v", err)
		}

		score := queue.ComputeScore(j.ScheduledAt, j.Priority)
		if err := q.EnqueueJob(ctx, j.ID, jobJSON, score); err != nil {
			log.Fatalf("failed to enqueue job: %v", err)
		}

		fmt.Printf("enqueued job: %s (priority=%d, scheduled_at=%s)\n", j.ID, j.Priority, j.ScheduledAt.Format(time.RFC3339))
	}
}