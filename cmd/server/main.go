package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/Oscur007/job-scheduler/internal/queue"
)

func main() {
	q := queue.NewRedisQueue("localhost:6379")
	ctx := context.Background()

	if err := q.Ping(ctx); err != nil {
		log.Fatalf("could not connect to redis: %v", err)
	}

	j := job.New("send_email", `{"to":"test@example.com"}`, 3)

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