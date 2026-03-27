WITH all_results (id) AS (
  SELECT id FROM results
  UNION
  SELECT source_result_id FROM links
)
SELECT 1 FROM all_results
WHERE id = ?
LIMIT 1;
