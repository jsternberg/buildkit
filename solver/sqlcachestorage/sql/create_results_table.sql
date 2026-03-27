CREATE TABLE IF NOT EXISTS results (
	id text NOT NULL,
  ref_id text NOT NULL,
	created_at timestamp,
  UNIQUE (id, ref_id)
);
