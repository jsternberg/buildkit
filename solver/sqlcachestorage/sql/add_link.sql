INSERT INTO links (source_result_id, vertex_input, vertex_output, vertex_digest, vertex_selector, target_result_id)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;
