CREATE INDEX IF NOT EXISTS links_index
ON links (source_result_id, vertex_input, vertex_output, vertex_digest, vertex_selector, target_result_id);
