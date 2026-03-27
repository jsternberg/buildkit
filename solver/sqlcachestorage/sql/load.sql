SELECT ref_id, created_at FROM results
WHERE id = ? AND ref_id = ?
LIMIT 1;
