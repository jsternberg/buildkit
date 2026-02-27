WITH
  referenced_results (id) AS (
    SELECT source_result_id FROM links
    UNION
    SELECT id FROM results
  )
DELETE FROM links
WHERE target_result_id NOT IN (
  SELECT id FROM referenced_results
);
