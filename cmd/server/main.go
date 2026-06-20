package main

import (
	"context"
	"fmt"
	"log"
	"github.com/Oscur007/job-scheduler/internal/queue"
)

func main() {
	q := queue.NewRedisQueue("localhost:6379")
	ctx := context.Background()

	if err := q.Ping(ctx); err != nil {
		log.Fatalf("could not connect to redis: %v", err)
	}
	fmt.Println("connected to redis successfully")
}