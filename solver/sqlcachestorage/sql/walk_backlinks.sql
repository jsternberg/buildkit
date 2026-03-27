SELECT
  source_result_id,
  vertex_input,
  vertex_output,
  vertex_digest,
  vertex_selector
FROM links
WHERE target_result_id = ?
ORDER BY
  source_result_id,
  vertex_input,
  vertex_output,
  vertex_digest,
  vertex_selector,
  target_result_id;
