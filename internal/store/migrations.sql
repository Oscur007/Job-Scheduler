CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    type          TEXT NOT NULL,
    payload       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    priority      INT NOT NULL DEFAULT 0,
    retries       INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 3,
    created_at    TIMESTAMP NOT NULL,
    scheduled_at  TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);