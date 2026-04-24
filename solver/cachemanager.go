package solver

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/cachedigest"
	digest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

// NewInMemoryCacheManager creates a new in-memory cache manager
func NewInMemoryCacheManager() CacheManager {
	return NewCacheManager(context.TODO(), identity.NewID(), NewInMemoryCacheStorage(), NewInMemoryResultStorage())
}

// NewCacheManager creates a new cache manager with specific storage backend
func NewCacheManager(ctx context.Context, id string, storage CacheKeyStorage, results CacheResultStorage) CacheManager {
	cm := &cacheManager{
		id:      id,
		storage: newKvCacheStorage(storage, results),
		backend: storage,
		results: results,
	}

	if err := cm.ReleaseUnreferenced(ctx); err != nil {
		bklog.G(ctx).Errorf("failed to release unreferenced cache metadata: %+v", err)
	}

	return cm
}

type cacheManager struct {
	mu sync.RWMutex
	id string

	storage CacheStorage
	backend CacheKeyStorage
	results CacheResultStorage
}

func (c *cacheManager) ReleaseUnreferenced(ctx context.Context) error {
	return c.storage.ReleaseUnreferenced(ctx)
}

func (c *cacheManager) ID() string {
	return c.id
}

func (c *cacheManager) Query(deps []CacheKeyWithSelector, input Index, dgst digest.Digest, output Index) (rcks []*CacheKey, rerr error) {
	depsField := make([]map[string]any, len(deps))
	for i, dep := range deps {
		depsField[i] = dep.TraceFields()
	}
	lg := bklog.G(context.TODO()).WithFields(logrus.Fields{
		"cache_manager": c.id,
		"op":            "query",
		"deps":          depsField,
		"input":         input,
		"digest":        dgst,
		"output":        output,
		"stack":         bklog.TraceLevelOnlyStack(),
	})
	defer func() {
		rcksField := make([]map[string]any, len(rcks))
		for i, rck := range rcks {
			rcksField[i] = rck.TraceFields()
		}
		lg.WithError(rerr).WithField("return_cachekeys", rcksField).Trace("cache manager")
	}()

	// Resolve the ids for all dependencies.
	for i, d := range deps {
		deps[i].CacheKey.CacheKey = c.getKey(d.CacheKey.CacheKey)
	}

	keys, err := c.storage.Query(deps, input, dgst, output)
	if err != nil {
		return nil, err
	}

	for i, ck := range keys {
		keys[i].keys[c] = ck
	}
	return keys, nil
}

func (c *cacheManager) Records(ctx context.Context, ck *CacheKey) (rrecs []*CacheRecord, rerr error) {
	lg := bklog.G(context.TODO()).WithFields(logrus.Fields{
		"cache_manager": c.id,
		"op":            "records",
		"cachekey":      ck.TraceFields(),
		"stack":         bklog.TraceLevelOnlyStack(),
	})
	defer func() {
		rrercsField := make([]map[string]any, len(rrecs))
		for i, rrec := range rrecs {
			rrercsField[i] = rrec.TraceFields()
		}
		lg.WithError(rerr).WithField("return_records", rrercsField).Trace("cache manager")
	}()

	outs, err := c.storage.Records(ctx, c.getKey(ck))
	if err != nil {
		return nil, err
	}

	for _, r := range outs {
		r.cacheManager = c
		r.key = ck
	}
	return outs, nil
}

func (c *cacheManager) Load(ctx context.Context, rec *CacheRecord) (rres Result, rerr error) {
	lg := bklog.G(context.TODO()).WithFields(logrus.Fields{
		"cache_manager": c.id,
		"op":            "load",
		"record":        rec.TraceFields(),
		"stack":         bklog.TraceLevelOnlyStack(),
	})
	defer func() {
		rresID := "<nil>"
		if rres != nil {
			rresID = rres.ID()
		}
		lg.WithError(rerr).WithField("return_result", rresID).Trace("cache manager")
	}()

	return c.storage.Load(ctx, c.getKey(rec.key), rec.ID)
}

type LoadedResult struct {
	Result      Result
	CacheResult CacheResult
	CacheKey    *CacheKey
}

func (r *LoadedResult) TraceFields() map[string]any {
	return map[string]any{
		"result":       r.Result.ID(),
		"cache_result": r.CacheResult.ID,
		"cache_key":    r.CacheKey.TraceFields(),
	}
}

// func (c *cacheManager) filterResults(m map[string]Result, ck *CacheKey, visited map[string]struct{}) (results []LoadedResult, err error) {
// 	id := c.getID(ck)
// 	if _, ok := visited[id]; ok {
// 		return nil, nil
// 	}
// 	visited[id] = struct{}{}
// 	if err := c.backend.WalkResults(id, func(cr CacheResult) error {
// 		res, ok := m[id]
// 		if ok {
// 			results = append(results, LoadedResult{
// 				Result:      res,
// 				CacheKey:    ck,
// 				CacheResult: cr,
// 			})
// 			delete(m, id)
// 		}
// 		return nil
// 	}); err != nil {
// 		for _, r := range results {
// 			r.Result.Release(context.TODO())
// 		}
// 	}
// 	for _, keys := range ck.Deps() {
// 		for _, key := range keys {
// 			res, err := c.filterResults(m, key.CacheKey.CacheKey, visited)
// 			if err != nil {
// 				for _, r := range results {
// 					r.Result.Release(context.TODO())
// 				}
// 				return nil, err
// 			}
// 			results = append(results, res...)
// 		}
// 	}
// 	return
// }

func (c *cacheManager) LoadWithParents(ctx context.Context, rec *CacheRecord) (rres []LoadedResult, rerr error) {
	lg := bklog.G(context.TODO()).WithFields(logrus.Fields{
		"cache_manager": c.id,
		"op":            "load_with_parents",
		"record":        rec.TraceFields(),
		"stack":         bklog.TraceLevelOnlyStack(),
	})
	defer func() {
		rresField := make([]map[string]any, len(rres))
		for i, rres := range rres {
			rresField[i] = rres.TraceFields()
		}
		lg.WithError(rerr).WithField("return_results", rresField).Trace("cache manager")
	}()

	res, err := c.Load(ctx, rec)
	if err != nil {
		return nil, err
	}
	key := c.getKey(rec.key)
	return []LoadedResult{{Result: res, CacheKey: rec.key, CacheResult: CacheResult{ID: key.ID, CreatedAt: rec.CreatedAt}}}, nil

	// lwp, ok := c.results.(interface {
	// 	LoadWithParents(context.Context, CacheResult) (map[string]Result, error)
	// })
	// if !ok {
	// 	res, err := c.Load(ctx, rec)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return []LoadedResult{{Result: res, CacheKey: rec.key, CacheResult: CacheResult{ID: c.getID(rec.key), CreatedAt: rec.CreatedAt}}}, nil
	// }
	// c.mu.RLock()
	// defer c.mu.RUnlock()
	//
	// cr, err := c.backend.Load(c.getID(rec.key), rec.ID)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// m, err := lwp.LoadWithParents(ctx, cr)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// results, err := c.filterResults(m, rec.key, map[string]struct{}{})
	// if err != nil {
	// 	for _, r := range m {
	// 		r.Release(context.TODO())
	// 	}
	// }
	// for _, r := range m {
	// 	// refs added to results are deleted from m by filterResults
	// 	// so release any leftovers
	// 	r.Release(context.TODO())
	// }
	//
	// return results, nil
}

func (c *cacheManager) Save(k *CacheKey, r Result, createdAt time.Time) (rck *ExportableCacheKey, err error) {
	lg := bklog.G(context.TODO()).WithFields(logrus.Fields{
		"cache_manager": c.id,
		"op":            "save",
		"result":        r.ID(),
		"stack":         bklog.TraceLevelOnlyStack(),
	})
	defer func() {
		if err != nil {
			lg = lg.WithError(err)
		} else {
			lg = lg.WithField("return_cachekey", rck.TraceFields())
		}
		lg.Trace("cache manager")
	}()

	k = c.getKey(k)
	rec, err := c.storage.Save(k, r, createdAt)
	if err != nil {
		return nil, err
	}
	rec.cacheManager = c
	rec.key = k

	return &ExportableCacheKey{
		CacheKey: k,
		Exporter: &exporter{k: k, record: rec},
	}, nil
}

func newKey() *CacheKey {
	return &CacheKey{
		keys: map[*cacheManager]*CacheKey{},
		ids:  map[*cacheManager]string{},
	}
}

func (c *cacheManager) getKey(k *CacheKey) *CacheKey {
	k.mu.Lock()
	ck, ok := k.keys[c]
	if ok {
		k.mu.Unlock()
		return ck
	}

	if len(k.deps) == 0 {
		k.keys[c] = k
		k.mu.Unlock()
		return k
	}

	ck = c.getKeyFromDeps(k)
	k.keys[c] = ck
	k.mu.Unlock()
	return ck
}

func (c *cacheManager) getKeyFromDeps(k *CacheKey) (ck *CacheKey) {
	ck = k
	if len(k.keys) > 0 {
		// Cannot reuse this cache key so duplicate it so we can resolve
		// the dependencies properly.
		ck = &CacheKey{
			digest: k.digest,
			vtx:    k.vtx,
			output: k.output,
			keys:   map[*cacheManager]*CacheKey{},
		}
		// Resolve each of the dependencies so their ID matches with their real identity.
		for _, dep := range k.deps {
			dep = slices.Clone(dep)
			ck.deps = append(ck.deps, dep)
		}
		ck.keys[c] = ck
	}

	// Resolve the dependency keys.
	for _, dep := range ck.deps {
		for i, d := range dep {
			dep[i].CacheKey.CacheKey = c.getKey(d.CacheKey.CacheKey)
		}
	}

	defer func() {
		// If the ID could not be resolved, use a random id.
		if ck.ID == "" {
			ck.ID = identity.NewID()
		}
	}()

	// Now that we've fully resolved our dependencies we can send a query request
	// to each of them to determine a suitable identity.
	keys, err := c.storage.Query(ck.deps[0], 0, ck.Digest(), ck.Output())
	if err != nil {
		return
	}

	matches := map[string]struct{}{}
	for _, k := range keys {
		matches[k.ID] = struct{}{}
	}

	for i, deps := range ck.deps[1:] {
		if len(matches) == 0 {
			break
		}

		keys, err := c.storage.Query(deps, Index(i+1), ck.Digest(), ck.Output())
		if err != nil {
			return
		}

		// Short circuit.
		if len(keys) == 0 {
			return
		}

		m2 := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			m2[k.ID] = struct{}{}
		}

		for id := range matches {
			if _, ok := m2[id]; !ok {
				delete(matches, id)
			}
		}
	}

	for k := range matches {
		ck.ID = k
		break
	}
	return
}

func rootKey(dgst digest.Digest, output Index) digest.Digest {
	out, _ := cachedigest.FromBytes(fmt.Appendf(nil, "%s@%d", dgst, output), cachedigest.TypeString)
	if strings.HasPrefix(dgst.String(), "random:") {
		return digest.Digest("random:" + dgst.Encoded())
	}
	return out
}
