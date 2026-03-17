package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/moby/buildkit/cache/remotecache"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/pkg/errors"
)

// ResolveCacheExporterFunc for "local" cache exporter.
func ResolveCacheExporterFunc() remotecache.ResolveCacheExporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, err
		}

		cc := v1.NewCacheChains()
		return &exporter{CacheExporterTarget: cc, chains: cc, config: config}, nil
	}
}

type exporter struct {
	solver.CacheExporterTarget
	chains *v1.CacheChains
	config Config
}

func (*exporter) Name() string {
	return "exporting cache to http"
}

func (e *exporter) Config() remotecache.Config {
	return remotecache.Config{
		Compression: compression.New(compression.Default),
	}
}

func (e *exporter) Finalize(ctx context.Context) (map[string]string, error) {
	cacheConfig, _, err := e.chains.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(cacheConfig)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/export", e.config.EndpointURL)
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

	var attrs map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&attrs); err != nil {
		return nil, err
	}
	return attrs, nil
}
