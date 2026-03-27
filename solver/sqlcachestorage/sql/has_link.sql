SELECT 1 FROM links
WHERE source_result_id = ?
AND vertex_input = ?
AND vertex_output = ?
AND vertex_digest = ?
AND vertex_selector = ?
AND target_result_id = ?
LIMIT 1;
