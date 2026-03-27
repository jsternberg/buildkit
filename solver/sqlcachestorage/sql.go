package sqlcachestorage

import _ "embed"

var (
	//go:embed sql/create_results_table.sql
	createResultsTableSQL string

	//go:embed sql/create_results_index.sql
	createResultsIndexSQL string

	//go:embed sql/create_links_table.sql
	createLinksTableSQL string

	//go:embed sql/create_links_index.sql
	createLinksIndexSQL string

	//go:embed sql/create_backlinks_index.sql
	createBacklinksIndexSQL string

	//go:embed sql/exists.sql
	existsSQL string

	//go:embed sql/walk.sql
	walkSQL string

	//go:embed sql/walk_distinct_results.sql
	walkDistinctResultsSQL string

	//go:embed sql/walk_results.sql
	walkResultsSQL string

	//go:embed sql/load.sql
	loadSQL string

	//go:embed sql/add_result.sql
	addResultSQL string

	//go:embed sql/delete_result.sql
	deleteResultSQL string

	//go:embed sql/walk_ids_by_result.sql
	walkIDsByResultSQL string

	//go:embed sql/add_link.sql
	addLinkSQL string

	//go:embed sql/walk_links.sql
	walkLinksSQL string

	//go:embed sql/has_link.sql
	hasLinkSQL string

	//go:embed sql/walk_backlinks.sql
	walkBacklinksSQL string

	//go:embed sql/delete_unreachable_links.sql
	deleteUnreachableLinksSQL string

	//go:embed sql/delete_unusable_links.sql
	deleteUnusableLinksSQL string
)

var createSQL = []string{
	createResultsTableSQL,
	createResultsIndexSQL,
	createLinksTableSQL,
	createLinksIndexSQL,
	createBacklinksIndexSQL,
}
