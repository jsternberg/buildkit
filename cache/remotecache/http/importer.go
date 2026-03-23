package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"time"

	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
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
		Digest:     outputKey(dgst, int(outputIndex)),
		InputIndex: inputIndex,
	}

	for _, ck := range inp {
		input := CacheKeyWithSelector{
			CacheKey: ck.CacheKey.ID,
			Selector: ck.Selector,
		}
		req.Inputs = append(req.Inputs, input)
	}

	dt, _ := json.Marshal(req)
	fmt.Println("query request:", string(dt))

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	urlStr := fmt.Sprintf("%s/query", cm.config.EndpointURL)
	hreq, _ := http.NewRequest("POST", urlStr, bytes.NewReader(body))
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

	dt, _ = io.ReadAll(resp.Body)
	fmt.Println("query response:", string(dt))
	debug.PrintStack()

	var queryResp QueryResponse
	if err := json.NewDecoder(bytes.NewReader(dt)).Decode(&queryResp); err != nil {
		return nil, err
	}

	keys := make([]*solver.CacheKey, 0, len(queryResp.CacheKeys))
	for _, ck := range queryResp.CacheKeys {
		newKey := solver.NewCacheKey(dgst, "", outputIndex)
		newKey.ID = ck
		keys = append(keys, newKey)
	}
	return keys, nil
}

func (cm *cacheManager) Records(ctx context.Context, ck *solver.CacheKey) ([]*solver.CacheRecord, error) {
	urlStr := fmt.Sprintf("%s/records", cm.config.EndpointURL)
	hreq, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("q", ck.ID)
	hreq.URL.RawQuery = params.Encode()
	hreq.Header.Set("Accept", "application/json")

	bklog.G(ctx).Infof("sending request to %s", hreq.URL.String())
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("unexpected http status code %s: %s", resp.Status, string(msg))
	}

	dt, _ := io.ReadAll(resp.Body)
	fmt.Println("records:", string(dt))

	var recordsResp RecordsResponse
	if err := json.NewDecoder(bytes.NewReader(dt)).Decode(&recordsResp); err != nil {
		return nil, err
	}

	records := make([]*solver.CacheRecord, 0, len(recordsResp.Records))
	for _, r := range recordsResp.Records {
		var createdAt time.Time
		if r.CreatedAt != nil {
			createdAt = *r.CreatedAt
		}
		records = append(records, &solver.CacheRecord{
			ID:        r.ID,
			CreatedAt: createdAt,
		})
	}
	return records, nil
}

func (cm *cacheManager) Load(ctx context.Context, rec *solver.CacheRecord) (solver.Result, error) {
	return nil, errors.New("implement me")
}

func (cm *cacheManager) Save(key *solver.CacheKey, s solver.Result, createdAt time.Time) (*solver.ExportableCacheKey, error) {
	return nil, nil
}

func (cm *cacheManager) ReleaseUnreferenced(ctx context.Context) error {
	return nil
}

func outputKey(dgst digest.Digest, idx int) digest.Digest {
	return digest.FromBytes(fmt.Appendf(nil, "%s@%d", dgst, idx))
}
