package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/moby/buildkit/cache/remotecache"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	cacheimporttypes "github.com/moby/buildkit/cache/remotecache/v1/types"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/progress"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ResolveCacheExporterFunc for "local" cache exporter.
func ResolveCacheExporterFunc() remotecache.ResolveCacheExporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, err
		}

		cc := v1.NewCacheChains()
		return &exporter{
			CacheExporterTarget: cc,
			client:              http.DefaultClient,
			chains:              cc,
			config:              config,
		}, nil
	}
}

type exporter struct {
	solver.CacheExporterTarget
	client *http.Client
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

type nopCloserSectionReader struct {
	*io.SectionReader
}

func newSectionReader(ra content.ReaderAt, n int64) io.Reader {
	return &nopCloserSectionReader{io.NewSectionReader(ra, 0, n)}
}

func (*nopCloserSectionReader) Close() error { return nil }

func (e *exporter) Finalize(ctx context.Context) (map[string]string, error) {
	cacheConfig, descs, err := e.chains.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	eg, groupCtx := errgroup.WithContext(ctx)
	tasks := make(chan int, e.config.UploadParallelism)

	go func() {
		for i := range cacheConfig.Layers {
			tasks <- i
		}
		close(tasks)
	}()

	for range e.config.UploadParallelism {
		eg.Go(func() error {
			for index := range tasks {
				blob := cacheConfig.Layers[index].Blob
				dgstPair, ok := descs[blob]
				if !ok {
					return errors.Errorf("missing blob %s", blob)
				}
				if dgstPair.Descriptor.Annotations == nil {
					return errors.Errorf("invalid descriptor without annotations")
				}
				v, ok := dgstPair.Descriptor.Annotations[labels.LabelUncompressed]
				if !ok {
					return errors.Errorf("invalid descriptor without uncompressed annotation")
				}
				diffID, err := digest.Parse(v)
				if err != nil {
					return errors.Wrapf(err, "failed to parse uncompressed annotation")
				}

				dgst := dgstPair.Descriptor.Digest
				// exists, size, err := e.s3Client.exists(groupCtx, key)
				// if err != nil {
				// 	return errors.Wrapf(err, "failed to check file presence in cache")
				// }
				// if exists != nil {
				// 	if time.Since(*exists) > e.config.TouchRefresh {
				// 		err = e.s3Client.touch(groupCtx, key, size)
				// 		if err != nil {
				// 			return errors.Wrapf(err, "failed to touch file")
				// 		}
				// 	}
				// } else {

				layerDone := progress.OneOff(groupCtx, fmt.Sprintf("writing layer %s", blob))
				ra, err := dgstPair.Provider.ReaderAt(groupCtx, dgstPair.Descriptor)
				if err != nil {
					return layerDone(errors.Wrap(err, "error reading layer blob from provider"))
				}
				defer ra.Close()
				if err := e.uploadLayer(groupCtx, dgst, newSectionReader(ra, ra.Size())); err != nil {
					return layerDone(errors.Wrap(err, "error writing layer blob"))
				}
				layerDone(nil)

				la := &cacheimporttypes.LayerAnnotations{
					DiffID:    diffID,
					Size:      dgstPair.Descriptor.Size,
					MediaType: dgstPair.Descriptor.MediaType,
				}
				if v, ok := dgstPair.Descriptor.Annotations["buildkit/createdat"]; ok {
					var t time.Time
					if err := (&t).UnmarshalText([]byte(v)); err != nil {
						return err
					}
					la.CreatedAt = t.UTC()
				}
				cacheConfig.Layers[index].Annotations = la
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
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

	resp, err := e.client.Do(hreq)
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

func (e *exporter) uploadLayer(ctx context.Context, dgst digest.Digest, body io.Reader) error {
	urlStr := fmt.Sprintf("%s/blobs/sha256/%s", e.config.EndpointURL, dgst.Encoded())
	req, err := http.NewRequestWithContext(ctx, "POST", urlStr, body)
	if err != nil {
		return err
	}
	req.Header.Set("Transfer-Encoding", "chunked")

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return errors.Errorf("unexpected http status code %s: %s", resp.Status, string(msg))
	}
	return nil
}
