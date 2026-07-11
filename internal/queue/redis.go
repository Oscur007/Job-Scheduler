package queue

import (
	"context"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
)

const (
	QueueKey = "jobs:queue"
	DataKey = "jobs:data"
	DLQKey = "jobs:dlq"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(addr string) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisQueue{client: client}
}

func (q *RedisQueue) EnqueueJob(ctx context.Context, jobID string, jobJSON string, score float64) error {
	pipe := q.client.TxPipeline()
	pipe.HSet(ctx, DataKey, jobID, jobJSON)
	pipe.ZAdd(ctx, QueueKey, redis.Z{Score: score, Member: jobID})
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) DequeueJob(ctx context.Context) (string, error) {
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

	jobJSON, err := q.client.HGet(ctx, DataKey, jobID).Result()
	if err != nil {
		return "", err
	}

	return jobJSON, nil
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

func (q *RedisQueue) EnqueueDLQ(ctx context.Context, jobID string) error {
	return q.client.LPush(ctx, DLQKey, jobID).Err()
}