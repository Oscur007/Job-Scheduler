package queue

import (
	"context"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
)

const QueueKey = "jobs:queue"

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(addr string) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisQueue{client: client}
}

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string, score float64) error {
	return q.client.ZAdd(ctx, QueueKey, redis.Z{
		Score:  score,
		Member: jobID,
	}).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context) (string, error) {
	now := float64(time.Now().Unix())

	result, err := q.client.ZRangeByScore(ctx, QueueKey, &redis.ZRangeBy{
		Min:   "0",
		Max:   fmt.Sprintf("%f", now),
		Count: 1,
	}).Result()
	if err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", redis.Nil
	}

	jobID := result[0]

	removed, err := q.client.ZRem(ctx, QueueKey, jobID).Result()
	if err != nil {
		return "", err
	}
	if removed == 0 {
		return "", redis.Nil
	}

	return jobID, nil
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}