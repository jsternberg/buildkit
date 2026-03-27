DELETE FROM links
WHERE source_result_id LIKE 'random:%'
  AND source_result_id NOT IN (SELECT id FROM results);
