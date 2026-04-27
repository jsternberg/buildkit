package sqlcachestorage

const (
	CREATE_TABLE_LINKS_SQL = `
CREATE TABLE IF NOT EXISTS links (
	source_record text NOT NULL,
	digest text NOT NULL,
	input_index integer NOT NULL,
	selector text,
	target_record text NOT NULL,
	UNIQUE (source_record, digest, input_index, selector, target_record)
);
`

	CREATE_TABLE_RECORDS_SQL = `
CREATE TABLE IF NOT EXISTS records (
	id text NOT NULL,
	record_id NOT NULL,
	created_at datetime,
	UNIQUE (id, record_id)
);
`

	CREATE_INDEX_LINKS_SOURCE_RECORD_SQL = `
CREATE INDEX IF NOT EXISTS links_source_record_index
ON links (source_record);
`

	CREATE_INDEX_RECORDS_ID_SQL = `
CREATE INDEX IF NOT EXISTS records_id_index
ON records (id);
`

	CREATE_INDEX_RECORDS_ID_AND_RECORD_SQL = `
CREATE INDEX IF NOT EXISTS records_id_index
ON records (id, record_id);
`

	INSERT_LINK_SQL = `
INSERT INTO links (source_record, digest, input_index, target_record, selector)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;
`

	INSERT_RECORD_SQL = `
INSERT INTO records (id, created_at, record_id)
VALUES (?, ?, ?)
ON CONFLICT DO NOTHING;
`

	LINK_EXISTS_SQL = `
SELECT 1 FROM links
WHERE source_record = ?
LIMIT 1;
`

	QUERY_LINKS_SQL = `
SELECT target_record
FROM links
WHERE source_record = ?
  AND digest = ?
  AND input_index = ?
  AND selector IS NULL
ORDER BY target_record ASC;
`

	QUERY_LINKS_WITH_SELECTOR_SQL = `
SELECT target_record
FROM links
WHERE source_record = ?
  AND digest = ?
  AND input_index = ?
  AND selector = ?
ORDER BY target_record ASC;
`

	SELECT_RECORDS_SQL = `
SELECT record_id, created_at
FROM records
WHERE id = ?
ORDER BY created_at DESC;
`

	SELECT_RECORD_SQL = `
SELECT created_at
FROM records
WHERE id = ?
  AND record_id = ?
ORDER BY created_at DESC
LIMIT 1;
`
)
