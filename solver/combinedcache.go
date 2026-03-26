package solver

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func NewCombinedCacheManager(cms []CacheManager, main CacheManager) CacheManager {
	return &combinedCacheManager{cms: cms, main: main}
}

type combinedCacheManager struct {
	cms    []CacheManager
	main   CacheManager
	id     string
	idOnce sync.Once
}

func (cm *combinedCacheManager) ID() string {
	cm.idOnce.Do(func() {
		ids := make([]string, len(cm.cms))
		for i, c := range cm.cms {
			ids[i] = c.ID()
		}
		cm.id = digest.FromBytes([]byte(strings.Join(ids, ","))).String()
	})
	return cm.id
}

func (cm *combinedCacheManager) ReleaseUnreferenced(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, c := range cm.cms {
		func(c CacheManager) {
			eg.Go(func() error {
				return c.ReleaseUnreferenced(ctx)
			})
		}(c)
	}
	return eg.Wait()
}

func (cm *combinedCacheManager) Query(inp []CacheKeyWithSelector, inputIndex Index, dgst digest.Digest, outputIndex Index) ([]*CacheKey, error) {
	eg, _ := errgroup.WithContext(context.TODO())
	keys := make(map[string]*CacheKey, len(cm.cms))
	var mu sync.Mutex
	for _, c := range cm.cms {
		func(c CacheManager) {
			eg.Go(func() error {
				dt, _ := json.Marshal(inp)
				bklog.G(context.TODO()).Debugf("cm %s: query for %v %d %s", c.ID(), string(dt), inputIndex, rootKey(dgst, outputIndex))

				recs, err := c.Query(inp, inputIndex, dgst, outputIndex)
				if err != nil {
					return err
				}

				names := make([]string, len(recs))
				for i, r := range recs {
					names[i] = r.ID
				}
				bklog.G(context.TODO()).Debugf("cm %s: records returned %d %v", c.ID(), len(recs), names)
				mu.Lock()
				for _, r := range recs {
					if _, ok := r.ids[c]; !ok {
						r.ids[c] = r.ID
					}

					if _, ok := keys[r.ID]; !ok || c == cm.main {
						keys[r.ID] = r
					}
				}
				mu.Unlock()
				return nil
			})
		}(c)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	out := make([]*CacheKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, k)
	}

	names := make([]string, 0, len(out))
	for _, k := range out {
		names = append(names, k.ID)
	}
	fmt.Printf("combined query results: %v\n", names)
	debug.PrintStack()
	return out, nil
}

func (cm *combinedCacheManager) Load(ctx context.Context, rec *CacheRecord) (res Result, err error) {
	var results []LoadedResult
	if cm, ok := rec.cacheManager.(*cacheManager); ok {
		results, err = cm.LoadWithParents(ctx, rec)
		if err != nil {
			return nil, err
		}
	} else {
		res, err := rec.cacheManager.Load(ctx, rec)
		if err != nil {
			return nil, err
		}
		results = []LoadedResult{{Result: res, CacheKey: rec.key, CacheResult: CacheResult{ID: rec.ID, CreatedAt: rec.CreatedAt}}}
	}

	defer func() {
		ctx := context.WithoutCancel(ctx)
		for i, res := range results {
			if err == nil && i == 0 {
				continue
			}
			res.Result.Release(ctx)
		}
	}()
	if rec.cacheManager != cm.main && cm.main != nil {
		for _, res := range results {
			bklog.G(ctx).Infof("saving %s to main cache with cache key %s", res.Result.ID(), res.CacheKey.ID)
			if _, err := cm.main.Save(res.CacheKey, res.Result, res.CacheResult.CreatedAt); err != nil {
				return nil, err
			}
		}
	}
	if len(results) == 0 { // TODO: handle gracefully
		return nil, errors.Errorf("failed to load deleted cache")
	}
	bklog.G(ctx).Info("successfully loaded cache record")
	return results[0].Result, nil
}

func (cm *combinedCacheManager) Save(key *CacheKey, s Result, createdAt time.Time) (*ExportableCacheKey, error) {
	if cm.main == nil {
		return nil, nil
	}
	return cm.main.Save(key, s, createdAt)
}

func (cm *combinedCacheManager) Records(ctx context.Context, ck *CacheKey) ([]*CacheRecord, error) {
	fmt.Println("combined cache records:", ck.ID)

	ck.mu.RLock()
	if len(ck.ids) == 0 {
		ck.mu.RUnlock()
		return nil, errors.Errorf("no results")
	}

	cms := make([]CacheManager, 0, len(ck.ids))
	for cm := range ck.ids {
		cms = append(cms, cm)
	}
	ck.mu.RUnlock()

	records := map[string]*CacheRecord{}
	var mu sync.Mutex

	eg, _ := errgroup.WithContext(context.TODO())
	for _, c := range cms {
		eg.Go(func() error {
			recs, err := c.Records(ctx, ck)
			if err != nil {
				return err
			}
			mu.Lock()
			for _, rec := range recs {
				rec.key = ck
				rec.cacheManager = c
				if _, ok := records[rec.ID]; !ok || c == cm.main {
					if c == cm.main {
						rec.Priority = 1
					}
					records[rec.ID] = rec
				}
			}
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	out := make([]*CacheRecord, 0, len(records))
	for _, rec := range records {
		out = append(out, rec)
	}
	return out, nil
}
