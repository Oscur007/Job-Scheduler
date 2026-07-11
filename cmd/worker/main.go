package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/Oscur007/job-scheduler/internal/queue"
	"github.com/Oscur007/job-scheduler/internal/store"
	"github.com/redis/go-redis/v9"
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

		fmt.Printf("processing job: %s (type=%s, attempt=%d)\n", j.ID, j.Type, j.Retries+1)

		if err := pgStore.UpdateStatus(ctx, j.ID, job.StatusRunning); err != nil {
			log.Printf("failed to update status to running: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
		success := rand.Intn(2) == 0

		if success {
			if err := pgStore.UpdateStatus(ctx, j.ID, job.StatusDone); err != nil {
				log.Printf("failed to update status to done: %v", err)
			}
			fmt.Printf("job %s marked done\n", j.ID)
			continue
		}

		if err := pgStore.IncrementRetries(ctx, j.ID); err != nil {
			log.Printf("failed to increment retries: %v", err)
		}
		j.Retries++

		if j.CanRetry() {
			backoff := math.Pow(2, float64(j.Retries))
			nextRun := time.Now().Add(time.Duration(backoff) * time.Second)
			score := float64(nextRun.Unix())

			updatedJSON, _ := j.Serialize()
			if err := q.EnqueueJob(ctx, j.ID, updatedJSON, score); err != nil {
				log.Printf("failed to re-enqueue job: %v", err)
			}

			if err := pgStore.UpdateStatus(ctx, j.ID, job.StatusPending); err != nil {
				log.Printf("failed to update status to pending: %v", err)
			}

			fmt.Printf("job %s failed, retrying in %.0fs (attempt %d/%d)\n", j.ID, backoff, j.Retries, j.MaxRetries)
		} else {
			if err := pgStore.UpdateStatus(ctx, j.ID, job.StatusFailed); err != nil {
				log.Printf("failed to update status to failed: %v", err)
			}
			if err := q.EnqueueDLQ(ctx, j.ID); err != nil {
				log.Printf("failed to enqueue to DLQ: %v", err)
			}
			fmt.Printf("job %s permanently failed after %d retries, moved to DLQ\n", j.ID, j.Retries)
		}
	}
}