package store

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/Oscur007/job-scheduler/internal/job"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) InsertJob(ctx context.Context, j *job.Job) error {
	query := `
		INSERT INTO jobs (id, type, payload, status, priority, retries, max_retries, created_at, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := s.db.ExecContext(ctx, query,
		j.ID, j.Type, j.Payload, j.Status, j.Priority, j.Retries, j.MaxRetries, j.CreatedAt, j.ScheduledAt,
	)
	return err
}

func (s *PostgresStore) UpdateStatus(ctx context.Context, jobID string, status job.Status) error {
	query := `UPDATE jobs SET status = $1, updated_at = now() WHERE id = $2`
	_, err := s.db.ExecContext(ctx, query, status, jobID)
	return err
}

func (s *PostgresStore) IncrementRetries(ctx context.Context, jobID string) error {
	query := `UPDATE jobs SET retries = retries + 1, updated_at = now() WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, jobID)
	return err
}

func (s *PostgresStore) GetJob(ctx context.Context, jobID string) (*job.Job, error) {
	query := `
		SELECT id, type, payload, status, priority, retries, max_retries, created_at, scheduled_at
		FROM jobs WHERE id = $1
	`
	row := s.db.QueryRowContext(ctx, query, jobID)

	var j job.Job
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.Retries, &j.MaxRetries, &j.CreatedAt, &j.ScheduledAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s", jobID)
		}
		return nil, err
	}
	return &j, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}