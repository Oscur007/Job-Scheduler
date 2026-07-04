package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	q := queue.NewRedisQueue("localhost:6379")
	ctx := context.Background()

	if err := q.Ping(ctx); err != nil {
		log.Fatalf("could not connect to redis: %v", err)
	}

	fmt.Println("worker started, polling for jobs...")

	for {
		jobJSON, err := q.DequeueJob(ctx)
		if err == redis.Nil {
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			log.Printf("error dequeuing job: %v", err)
			continue
		}

		j, err := job.Deserialize(jobJSON)
		if err != nil {
			log.Printf("error deserializing job: %v", err)
			continue
		}

		fmt.Printf("processing job: %s (type=%s, payload=%s)\n", j.ID, j.Type, j.Payload)
	}
}