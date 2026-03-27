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

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/moby/buildkit/cache/remotecache"
	cacheimport "github.com/moby/buildkit/cache/remotecache/v1"
	cacheimporttypes "github.com/moby/buildkit/cache/remotecache/v1/types"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/worker"
	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const pkgpath = "github.com/moby/buildkit/cache/remotecache/http"

// ResolveCacheImporterFunc for "local" cache importer.
func ResolveCacheImporterFunc(tp trace.TracerProvider) remotecache.ResolveCacheImporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Importer, ocispecs.Descriptor, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, ocispecs.Descriptor{}, err
		}
		return &importer{config, tp}, ocispecs.Descriptor{}, nil
	}
}

type importer struct {
	config Config
	tp     trace.TracerProvider
}

func (i *importer) Resolve(ctx context.Context, desc ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	cm := &cacheManager{
		id:     id,
		config: i.config,
		w:      w,
	}
	if i.tp != nil {
		cm.tracer = i.tp.Tracer(pkgpath)
	}
	return cm, nil
}

type cacheManager struct {
	id     string
	config Config
	w      worker.Worker
	tracer trace.Tracer
}

func (cm *cacheManager) ID() string {
	return cm.id
}

func (cm *cacheManager) Query(ctx context.Context, inp []solver.CacheKeyWithSelector, inputIndex solver.Index, dgst digest.Digest, outputIndex solver.Index) ([]*solver.CacheKey, error) {
	if cm.tracer != nil {
		var span trace.Span
		ctx, span = cm.tracer.Start(ctx, "(*cacheManager).Query",
			trace.WithAttributes(
				attribute.String("cache_manager", cm.id),
				attribute.Int("input", int(inputIndex)),
				attribute.String("digest", dgst.String()),
				attribute.Int("output", int(outputIndex)),
			))
		defer span.End()
	}

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
	if cm.tracer != nil {
		var span trace.Span
		ctx, span = cm.tracer.Start(ctx, "(*cacheManager).Records",
			trace.WithAttributes(
				attribute.String("cache_manager", cm.id),
			))
		defer span.End()
	}

	urlStr := fmt.Sprintf("%s/records", cm.config.EndpointURL)
	hreq, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
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
	if cm.tracer != nil {
		var span trace.Span
		ctx, span = cm.tracer.Start(ctx, "(*cacheManager).Load",
			trace.WithAttributes(
				attribute.String("cache_manager", cm.id),
			))
		defer span.End()
	}

	layers, err := cm.loadManifest(ctx, rec)
	if err != nil {
		return nil, err
	}
	bklog.G(ctx).Infof("loading cache record %s with %d layers", rec.ID, len(layers))

	mp := contentutil.NewMultiProvider(nil)
	remote := &solver.Remote{}
	for _, l := range layers {
		descPair := cacheimport.DescriptorProviderPair{
			Descriptor: ocispecs.Descriptor{
				MediaType: l.Annotations.MediaType,
				Digest:    l.Blob,
				Size:      l.Annotations.Size,
				Annotations: map[string]string{
					labels.LabelUncompressed: l.Annotations.DiffID.String(),
				},
			},
			Provider: cm,
		}
		if !l.Annotations.CreatedAt.IsZero() {
			txt, err := l.Annotations.CreatedAt.MarshalText()
			if err != nil {
				return nil, err
			}
			descPair.Descriptor.Annotations["buildkit/createdat"] = string(txt)
		}
		remote.Descriptors = append(remote.Descriptors, descPair.Descriptor)
		mp.Add(descPair.Descriptor.Digest, descPair)
	}
	remote.Provider = mp

	bklog.G(ctx).Infof("constructed solver result for %s", rec.ID)
	ref, err := cm.w.FromRemote(ctx, remote)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load result from remote")
	}
	bklog.G(ctx).Infof("immutable reference from remote: %#v", ref)
	return worker.NewWorkerRefResult(ref, cm.w), nil
}

func (cm *cacheManager) loadManifest(ctx context.Context, rec *solver.CacheRecord) ([]*cacheimporttypes.CacheLayer, error) {
	urlStr := fmt.Sprintf("%s/record", cm.config.EndpointURL)
	hreq, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("q", rec.ID)
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

	var recordResp RecordResponse
	if err := json.NewDecoder(resp.Body).Decode(&recordResp); err != nil {
		return nil, err
	}
	return recordResp.Layers, nil
}

func (cm *cacheManager) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
	bklog.G(ctx).Info("attempted to call reader at but it hasn't been implemented yet")
	debug.PrintStack()
	return nil, errors.New("implement reader at")
}

func (cm *cacheManager) Save(key *solver.CacheKey, s solver.Result, createdAt time.Time) (*solver.ExportableCacheKey, error) {
	return nil, errors.Errorf("importer is immutable")
}

func (cm *cacheManager) ReleaseUnreferenced(ctx context.Context) error {
	return nil
}

func outputKey(dgst digest.Digest, idx int) digest.Digest {
	return digest.FromBytes(fmt.Appendf(nil, "%s@%d", dgst, idx))
}

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

type readerAtCloser struct {
	offset int64
	rc     io.ReadCloser
	ra     io.ReaderAt
	open   func(offset int64) (io.ReadCloser, error)
	closed bool
}

func toReaderAtCloser(open func(offset int64) (io.ReadCloser, error)) ReaderAtCloser {
	return &readerAtCloser{
		open: open,
	}
}

func (hrs *readerAtCloser) ReadAt(p []byte, off int64) (n int, err error) {
	if hrs.closed {
		return 0, io.EOF
	}

	if hrs.ra != nil {
		return hrs.ra.ReadAt(p, off)
	}

	if hrs.rc == nil || off != hrs.offset {
		if hrs.rc != nil {
			hrs.rc.Close()
			hrs.rc = nil
		}
		rc, err := hrs.open(off)
		if err != nil {
			return 0, err
		}
		hrs.rc = rc
	}
	if ra, ok := hrs.rc.(io.ReaderAt); ok {
		hrs.ra = ra
		n, err = ra.ReadAt(p, off)
	} else {
		for {
			var nn int
			nn, err = hrs.rc.Read(p)
			n += nn
			p = p[nn:]
			if nn == len(p) || err != nil {
				break
			}
		}
	}

	hrs.offset += int64(n)
	return
}

func (hrs *readerAtCloser) Close() error {
	if hrs.closed {
		return nil
	}
	hrs.closed = true
	if hrs.rc != nil {
		return hrs.rc.Close()
	}

	return nil
}

type readerAt struct {
	ReaderAtCloser
	size int64
}

func (r *readerAt) Size() int64 {
	return r.size
}
