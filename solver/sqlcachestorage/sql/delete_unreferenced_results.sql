WITH
result_references (id, worker_ref_count, link_source_ref_count) AS (
  SELECT
    R.id AS id,
    COUNT(WR.result_id) AS worker_ref_count,
    COUNT(LS.source_result_id) AS link_source_ref_count
  FROM results AS R
  LEFT JOIN worker_results AS WR ON R.id = WR.result_id
  LEFT JOIN links AS LS ON R.id = LS.source_result_id
  GROUP BY R.id
),
unreferenced_results (id) AS (
  SELECT id
  FROM result_references
  WHERE worker_ref_count = 0
    AND link_source_ref_count = 0
)
DELETE FROM results AS R
WHERE EXISTS (
  SELECT 1 FROM unreferenced_results AS UR
  WHERE R.id = UR.id
);
