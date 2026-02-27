//go:build sqlite

package main

import (
	"path/filepath"

	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/sqlcachestorage"
)

func newCacheKeyStorage(root string) (solver.PersistentCacheKeyStorage, error) {
	return sqlcachestorage.NewStore(filepath.Join(root, "cache.db.sql"))
}
