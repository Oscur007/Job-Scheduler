package job

import (
	"encoding/json"
	"time"
	"github.com/google/uuid"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Job struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Payload     string    `json:"payload"`
	Status      Status    `json:"status"`
	Priority    int       `json:"priority"`
	Retries     int       `json:"retries"`
	MaxRetries  int       `json:"max_retries"`
	CreatedAt   time.Time `json:"created_at"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

func New(jobType, payload string, maxRetries, priority int, delay time.Duration) *Job {
	now := time.Now()
	return &Job{
		ID:          uuid.NewString(),
		Type:        jobType,
		Payload:     payload,
		Status:      StatusPending,
		Priority:    priority,
		Retries:     0,
		MaxRetries:  maxRetries,
		CreatedAt:   now,
		ScheduledAt: now.Add(delay),
	}
}

func (j *Job) Serialize() (string, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func Deserialize(data string) (*Job, error) {
	var j Job
	if err := json.Unmarshal([]byte(data), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (j *Job) CanRetry() bool {
	return j.Retries < j.MaxRetries
}