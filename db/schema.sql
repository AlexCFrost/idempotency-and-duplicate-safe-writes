CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE incidents (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID NOT NULL,
  service_id  UUID NOT NULL,
  title       TEXT NOT NULL,
  severity    TEXT NOT NULL CHECK (severity IN ('P1','P2','P3','P4')),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
  tenant_id       UUID NOT NULL,
  operation       TEXT NOT NULL,
  key             TEXT NOT NULL,
  request_hash    TEXT NOT NULL,
  state           TEXT NOT NULL CHECK (state IN ('processing','completed','failed')),
  response_status INTEGER,
  response_body   JSONB,
  expires_at      TIMESTAMPTZ NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation, key)
);

CREATE INDEX idempotency_keys_expires_at_idx ON idempotency_keys(expires_at);

CREATE TABLE paging_jobs (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  incident_id UUID NOT NULL UNIQUE REFERENCES incidents(id) ON DELETE CASCADE,
  tenant_id   UUID NOT NULL,
  job_type    TEXT NOT NULL DEFAULT 'page_incident',
  stable_id   UUID NOT NULL UNIQUE,
  state       TEXT NOT NULL DEFAULT 'pending',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
