package queue

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Oscur007/job-scheduler/internal/job"
	"github.com/redis/go-redis/v9"
)

const testDB = 15

func newTestQueue(t *testing.T) *RedisQueue {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	q := NewRedisQueueDB(addr, testDB)
	ctx := context.Background()
	if err := q.Ping(ctx); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := q.client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("could not clear test database: %v", err)
	}
	t.Cleanup(func() {
		q.client.FlushDB(ctx)
		q.client.Close()
	})
	return q
}

func enqueue(t *testing.T, q *RedisQueue, j *job.Job) {
	t.Helper()
	data, err := j.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	score := ComputeScore(j.ScheduledAt, j.Priority)
	if err := q.EnqueueJob(context.Background(), j.ID, data, score); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

func mustDequeue(t *testing.T, q *RedisQueue) string {
	t.Helper()
	data, err := q.DequeueJob(context.Background())
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	j, err := job.Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	return j.ID
}

func pastJob(priority int, at time.Time) *job.Job {
	j := job.New("test", "{}", 3, priority, 0)
	j.ScheduledAt = at
	return j
}

func TestDequeue_EmptyQueue(t *testing.T) {
	q := newTestQueue(t)

	_, err := q.DequeueJob(context.Background())
	if !errors.Is(err, redis.Nil) {
		t.Errorf("dequeue on empty queue: err = %v, want redis.Nil", err)
	}
}

func TestEnqueueDequeue_RoundTrip(t *testing.T) {
	q := newTestQueue(t)

	j := job.New("send_email", `{"to":"a@b.com"}`, 3, 0, 0)
	enqueue(t, q, j)

	data, err := q.DequeueJob(context.Background())
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	got, err := job.Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if got.ID != j.ID || got.Type != j.Type || got.Payload != j.Payload {
		t.Errorf("got %+v, want %+v", got, j)
	}
}

func TestDequeue_JobScheduledNowIsReadyImmediately(t *testing.T) {
	q := newTestQueue(t)

	for _, priority := range []int{0, 5, 100} {
		j := job.New("test", "{}", 3, priority, 0)
		enqueue(t, q, j)

		if got := mustDequeue(t, q); got != j.ID {
			t.Errorf("priority %d: dequeued %s, want %s", priority, got, j.ID)
		}
	}
}

func TestDequeue_DelayedJobIsNotReadyYet(t *testing.T) {
	q := newTestQueue(t)

	j := job.New("test", "{}", 3, 0, time.Hour)
	enqueue(t, q, j)

	_, err := q.DequeueJob(context.Background())
	if !errors.Is(err, redis.Nil) {
		t.Errorf("delayed job was dequeued early: err = %v, want redis.Nil", err)
	}
}

func TestDequeue_HigherPriorityFirst(t *testing.T) {
	q := newTestQueue(t)
	at := time.Now().Add(-time.Minute)

	low := pastJob(0, at)
	high := pastJob(10, at)
	enqueue(t, q, low)
	enqueue(t, q, high)

	if got := mustDequeue(t, q); got != high.ID {
		t.Errorf("first dequeued = %s, want the high-priority job %s", got, high.ID)
	}
	if got := mustDequeue(t, q); got != low.ID {
		t.Errorf("second dequeued = %s, want the low-priority job %s", got, low.ID)
	}
}

func TestDequeue_EarlierJobBeatsHigherPriority(t *testing.T) {
	q := newTestQueue(t)
	now := time.Now()

	early := pastJob(0, now.Add(-time.Hour))
	late := pastJob(100, now.Add(-time.Minute))
	enqueue(t, q, late)
	enqueue(t, q, early)

	if got := mustDequeue(t, q); got != early.ID {
		t.Errorf("first dequeued = %s, want the earlier job %s", got, early.ID)
	}
}

func TestDequeue_ConcurrentConsumersNeverShareAJob(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	at := time.Now().Add(-time.Minute)

	const total = 50
	for i := 0; i < total; i++ {
		enqueue(t, q, pastJob(0, at))
	}

	var mu sync.Mutex
	seen := map[string]int{}

	var wg sync.WaitGroup
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				data, err := q.DequeueJob(ctx)
				if errors.Is(err, redis.Nil) {
					n, cErr := q.client.ZCard(ctx, QueueKey).Result()
					if cErr != nil || n == 0 {
						return
					}
					continue
				}
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				j, err := job.Deserialize(data)
				if err != nil {
					t.Errorf("deserialize: %v", err)
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("processed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %s was dequeued %d times, want exactly once", id, n)
		}
	}
}

func TestDLQ(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.EnqueueDLQ(ctx, "job-1"); err != nil {
		t.Fatalf("EnqueueDLQ: %v", err)
	}

	ids, err := q.client.LRange(ctx, DLQKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(ids) != 1 || ids[0] != "job-1" {
		t.Errorf("DLQ contents = %v, want [job-1]", ids)
	}
}

func TestReapExpired_ReturnsOnlyExpiredJobs(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.MarkProcessing(ctx, "crashed-job", -5*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := q.MarkProcessing(ctx, "healthy-job", time.Hour); err != nil {
		t.Fatal(err)
	}

	expired, err := q.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if len(expired) != 1 || expired[0] != "crashed-job" {
		t.Errorf("reaped %v, want [crashed-job]", expired)
	}

	again, err := q.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("second ReapExpired: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second reap returned %v, want nothing", again)
	}
}

func TestCompleteProcessing_PreventsReaping(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.MarkProcessing(ctx, "job-1", -5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := q.CompleteProcessing(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}

	expired, err := q.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if len(expired) != 0 {
		t.Errorf("reaped %v, want nothing (job was completed)", expired)
	}
}

func TestCrashRecovery_JobIsRequeuedAfterWorkerDies(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	j := job.New("send_email", `{"to":"a@b.com"}`, 3, 5, 0)
	enqueue(t, q, j)

	if got := mustDequeue(t, q); got != j.ID {
		t.Fatalf("dequeued %s, want %s", got, j.ID)
	}
	if err := q.MarkProcessing(ctx, j.ID, -5*time.Second); err != nil {
		t.Fatal(err)
	}

	if _, err := q.DequeueJob(ctx); !errors.Is(err, redis.Nil) {
		t.Fatalf("queue should be empty after the claim, got err = %v", err)
	}

	expired, err := q.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if len(expired) != 1 || expired[0] != j.ID {
		t.Fatalf("reaped %v, want [%s]", expired, j.ID)
	}
	data, err := q.GetJobData(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJobData: %v", err)
	}
	recovered, err := job.Deserialize(data)
	if err != nil {
		t.Fatal(err)
	}
	score := ComputeScore(time.Now().Add(-time.Second), recovered.Priority)
	if err := q.EnqueueJob(ctx, recovered.ID, data, score); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	if got := mustDequeue(t, q); got != j.ID {
		t.Errorf("after recovery dequeued %s, want %s", got, j.ID)
	}
}