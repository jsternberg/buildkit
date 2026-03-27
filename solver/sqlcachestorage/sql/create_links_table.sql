CREATE TABLE IF NOT EXISTS links (
	source_result_id text NOT NULL,
	vertex_input integer NOT NULL,
	vertex_output integer NOT NULL,
	vertex_digest text NOT NULL,
	vertex_selector text DEFAULT '',
	target_result_id text NOT NULL,
  UNIQUE (source_result_id, vertex_input, vertex_output, vertex_digest, vertex_selector, target_result_id)
);
