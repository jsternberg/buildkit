//go:build !sqlite

package main

import (
	"path/filepath"

	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/bboltcachestorage"
)

func newCacheKeyStorage(root string) (solver.PersistentCacheKeyStorage, error) {
	return bboltcachestorage.NewStore(filepath.Join(root, "cache.db"))
}
