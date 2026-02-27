SELECT
  ref_id,
  created_at
FROM results
WHERE id = ?
ORDER BY ref_id;
