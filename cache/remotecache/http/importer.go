package http

import (
	"context"
	"time"

	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/worker"
	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

// ResolveCacheImporterFunc for "local" cache importer.
func ResolveCacheImporterFunc(sm *session.Manager) remotecache.ResolveCacheImporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Importer, ocispecs.Descriptor, error) {
		return &importer{}, ocispecs.Descriptor{}, nil
	}
}

type importer struct{}

func (i *importer) Resolve(ctx context.Context, desc ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	return &cacheManager{id: id}, nil
}

type cacheManager struct {
	id string
}

func (cm *cacheManager) ID() string {
	return cm.id
}

func (cm *cacheManager) Query(inp []solver.CacheKeyWithSelector, inputIndex solver.Index, dgst digest.Digest, outputIndex solver.Index) ([]*solver.CacheKey, error) {
	return nil, nil
}

func (cm *cacheManager) Records(ctx context.Context, ck *solver.CacheKey) ([]*solver.CacheRecord, error) {
	return nil, nil
}

func (cm *cacheManager) Load(ctx context.Context, rec *solver.CacheRecord) (solver.Result, error) {
	return nil, nil
}

func (cm *cacheManager) Save(key *solver.CacheKey, s solver.Result, createdAt time.Time) (*solver.ExportableCacheKey, error) {
	return nil, nil
}

func (cm *cacheManager) ReleaseUnreferenced(ctx context.Context) error {
	return nil
}
