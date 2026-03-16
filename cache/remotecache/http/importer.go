package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/worker"
	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// ResolveCacheImporterFunc for "local" cache importer.
func ResolveCacheImporterFunc() remotecache.ResolveCacheImporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Importer, ocispecs.Descriptor, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, ocispecs.Descriptor{}, err
		}
		return &importer{config}, ocispecs.Descriptor{}, nil
	}
}

type importer struct {
	config Config
}

func (i *importer) Resolve(ctx context.Context, desc ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	return &cacheManager{
		id:     id,
		config: i.config,
	}, nil
}

type cacheManager struct {
	id     string
	config Config
}

func (cm *cacheManager) ID() string {
	return cm.id
}

func (cm *cacheManager) Query(inp []solver.CacheKeyWithSelector, inputIndex solver.Index, dgst digest.Digest, outputIndex solver.Index) ([]*solver.CacheKey, error) {
	req := &QueryRequest{
		Digest:      dgst,
		InputIndex:  inputIndex,
		OutputIndex: outputIndex,
	}

	m := map[string]int{}
	var getCacheKeyIndex func(k *solver.CacheKey) int
	getCacheKeyIndex = func(k *solver.CacheKey) int {
		if index, ok := m[k.ID]; ok {
			return index
		}

		depKey := &CacheKey{
			ID: k.ID,
		}
		for _, dep := range k.Deps() {
			inpDeps := make([]CacheKeyWithSelector, 0, len(dep))
			for _, d := range dep {
				inpDeps = append(inpDeps, CacheKeyWithSelector{
					CacheKey: getCacheKeyIndex(d.CacheKey.CacheKey),
					Selector: d.Selector,
				})
			}
			depKey.Deps = append(depKey.Deps, inpDeps)
		}

		m[k.ID] = len(req.CacheKeys)
		req.CacheKeys = append(req.CacheKeys, depKey)
		return len(req.CacheKeys) - 1
	}

	for _, ck := range inp {
		input := CacheKeyWithSelector{
			CacheKey: getCacheKeyIndex(ck.CacheKey.CacheKey),
			Selector: ck.Selector,
		}
		req.Inputs = append(req.Inputs, input)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/query", cm.config.EndpointURL)
	hreq, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("unexpected http status code %s: %s", resp.Status, string(msg))
	}

	dt, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(dt))
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
