package plugin

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/pkg/labels"
	cacheservice "github.com/moby/buildkit/api/services/cache"
	"github.com/moby/buildkit/cache/remotecache"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	cacheimporttypes "github.com/moby/buildkit/cache/remotecache/v1/types"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/util/grpcerrors"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/worker"
	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	pkgpath               = "github.com/moby/buildkit/cache/remotecache/plugin"
	attrName              = "name"
	attrUploadParallelism = "upload_parallelism"
)

type Config struct {
	Name              string
	UploadParallelism int
}

func getConfig(attrs map[string]string) (Config, error) {
	name, ok := attrs[attrName]
	if !ok {
		return Config{}, errors.Errorf("name not set for remotecache plugin")
	}

	uploadParallelism := 4
	uploadParallelismStr, ok := attrs[attrUploadParallelism]
	if ok {
		uploadParallelismInt, err := strconv.Atoi(uploadParallelismStr)
		if err != nil {
			return Config{}, errors.Errorf("upload_parallelism must be a positive integer")
		}
		if uploadParallelismInt <= 0 {
			return Config{}, errors.Errorf("upload_parallelism must be a positive integer")
		}
		uploadParallelism = uploadParallelismInt
	}

	return Config{
		Name:              name,
		UploadParallelism: uploadParallelism,
	}, nil
}

type pluginResolver struct {
	tp      trace.TracerProvider
	plugins map[string]*plugin
	mu      sync.Mutex
}

func newPluginResolver(tp trace.TracerProvider) *pluginResolver {
	return &pluginResolver{tp: tp}
}

func (p *pluginResolver) Resolve(ctx context.Context, attrs map[string]string) (*plugin, error) {
	config, err := getConfig(attrs)
	if err != nil {
		return nil, err
	}
	name := config.Name

	p.mu.Lock()
	defer p.mu.Unlock()

	if pl, ok := p.plugins[name]; ok {
		return pl, nil
	}

	pr, pw := net.Pipe()
	cmd := exec.Command(name)
	cmd.Stdin = pr
	cmd.Stdout = pr

	cc, err := grpc.NewClient("passthrough://",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return pw, nil
		}),
	)
	if err != nil {
		return nil, err
	}

	pl := &plugin{
		cc:     cc,
		config: config,
	}
	if p.plugins == nil {
		p.plugins = make(map[string]*plugin)
	}
	if err := pl.Initialize(ctx, attrs); err != nil {
		if closeErr := pl.Close(); closeErr != nil {
			bklog.G(ctx).Warnf("plugin close failed after a failed initialization: %s", closeErr)
		}
		return nil, err
	}
	p.plugins[name] = pl
	return pl, nil
}

func (p *pluginResolver) ImporterFunc(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Importer, ocispecs.Descriptor, error) {
	pl, err := p.Resolve(ctx, attrs)
	if err != nil {
		return nil, ocispecs.Descriptor{}, err
	}
	return pl, ocispecs.Descriptor{}, nil
}

func (p *pluginResolver) ExporterFunc(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
	pl, err := p.Resolve(ctx, attrs)
	if err != nil {
		return nil, err
	}

	cc := v1.NewCacheChains()
	return &exporter{
		CacheExporterTarget: cc,
		cc:                  pl.cc,
		chains:              cc,
		config:              pl.config,
	}, nil
}

func (p *pluginResolver) Close() error {
	return nil
}

type plugin struct {
	cc     *grpc.ClientConn
	config Config
	tp     trace.TracerProvider
}

func (p *plugin) Initialize(ctx context.Context, attrs map[string]string) error {
	cm := cacheservice.NewCacheManagerClient(p.cc)

	req := &cacheservice.InitializeRequest{}
	for k, v := range attrs {
		req.Attrs = append(req.Attrs, &cacheservice.KeyValue{
			Key:   k,
			Value: v,
		})
	}

	_, err := cm.Initialize(ctx, req)
	return err
}

func (p *plugin) Resolve(ctx context.Context, desc ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	cm := &cacheManager{
		id:     id,
		config: p.config,
		w:      w,
	}
	if p.tp != nil {
		cm.tracer = p.tp.Tracer(pkgpath)
	}
	return cm, nil
}

func (p *plugin) Close() error {
	return nil
}

func ResolveCacheFuncs(tp trace.TracerProvider) (remotecache.ResolveCacheImporterFunc, remotecache.ResolveCacheExporterFunc, func() error) {
	pr := newPluginResolver(tp)
	return pr.ImporterFunc, pr.ExporterFunc, pr.Close
}

type importer struct {
	cc     *grpc.ClientConn
	config Config
	tp     trace.TracerProvider
}

func (i *importer) Resolve(ctx context.Context, desc ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	cm := &cacheManager{
		id:     id,
		config: i.config,
		w:      w,
		cm:     cacheservice.NewCacheManagerClient(i.cc),
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

	cm cacheservice.CacheManagerClient
}

func (cm *cacheManager) ID() string {
	return cm.id
}

func (cm *cacheManager) Query(ctx context.Context, deps []solver.CacheKeyWithSelector, inputIndex solver.Index, dgst digest.Digest, outputIndex solver.Index) ([]*solver.CacheKey, error) {
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

	req := &cacheservice.QueryRequest{
		Digest:     string(outputKey(dgst, int(outputIndex))),
		InputIndex: int32(inputIndex),
	}
	for _, dep := range deps {
		req.Deps = append(req.Deps, &cacheservice.CacheKeyWithSelector{
			Id:       dep.CacheKey.ID,
			Selector: string(dep.Selector),
		})
	}

	resp, err := cm.cm.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	keys := make([]*solver.CacheKey, 0, len(resp.CacheKeys))
	for _, ck := range resp.CacheKeys {
		k := solver.NewCacheKey(dgst, "", outputIndex)
		k.ID = ck
		keys = append(keys, k)
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

	req := &cacheservice.RecordsRequest{Id: ck.ID}
	resp, err := cm.cm.Records(ctx, req)
	if err != nil {
		return nil, err
	}

	records := make([]*solver.CacheRecord, 0, len(resp.Records))
	for _, r := range resp.Records {
		var createdAt time.Time
		if r.CreatedAt != 0 {
			createdAt = time.Unix(r.CreatedAt, 0).UTC()
		}
		records = append(records, &solver.CacheRecord{
			ID:        r.Id,
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

	mp := contentutil.NewMultiProvider(nil)
	remote := &solver.Remote{}
	for _, l := range layers {
		descPair := v1.DescriptorProviderPair{
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

	ref, err := cm.w.FromRemote(ctx, remote)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load result from remote")
	}
	return worker.NewWorkerRefResult(ref, cm.w), nil
}

func (cm *cacheManager) loadManifest(ctx context.Context, rec *solver.CacheRecord) ([]*cacheimporttypes.CacheLayer, error) {
	req := &cacheservice.RecordRequest{Id: rec.ID}
	resp, err := cm.cm.Record(ctx, req)
	if err != nil {
		return nil, err
	}

	layers := make([]*cacheimporttypes.CacheLayer, len(resp.Layers))
	indexByDigest := make(map[digest.Digest]int)
	for i, layer := range resp.Layers {
		l := &cacheimporttypes.CacheLayer{
			Blob: digest.Digest(layer.BlobDigest),
			// Fill this in later after seeing the digests.
			// This is the default value if we don't have a parent.
			ParentIndex: -1,
			Annotations: &cacheimporttypes.LayerAnnotations{
				MediaType: layer.MediaType,
				DiffID:    digest.Digest(layer.DiffId),
				Size:      int64(layer.Size),
				CreatedAt: time.Unix(layer.CreatedAt, 0).UTC(),
			},
		}
		layers[i] = l
		indexByDigest[l.Blob] = i
	}

	for i, layer := range resp.Layers {
		if layer.ParentBlobDigest == "" {
			continue
		}

		index, ok := indexByDigest[digest.Digest(layer.ParentBlobDigest)]
		if !ok {
			return nil, errors.Errorf("parent digest %s for %s not included in the returned layers", layer.ParentBlobDigest, layer.BlobDigest)
		}
		layers[i].ParentIndex = index
	}
	return layers, nil
}

func (cm *cacheManager) Save(key *solver.CacheKey, s solver.Result, createdAt time.Time) (*solver.ExportableCacheKey, error) {
	return nil, errors.Errorf("importer is immutable")
}

func (cm *cacheManager) ReleaseUnreferenced(ctx context.Context) error {
	return nil
}

type exporter struct {
	solver.CacheExporterTarget
	cc     *grpc.ClientConn
	chains *v1.CacheChains
	config Config
}

func (e *exporter) Name() string {
	return fmt.Sprintf("exporting cache to %s", e.config.Name)
}

func (e *exporter) Config() remotecache.Config {
	return remotecache.Config{
		Compression: compression.New(compression.Default),
	}
}

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

	cm := cacheservice.NewCacheManagerClient(e.cc)
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

				if _, err := cm.LayerInfo(groupCtx, &cacheservice.LayerInfoRequest{
					Digest: string(dgstPair.Descriptor.Digest),
				}); err != nil {
					if grpcerrors.Code(err) != codes.NotFound {
						return errors.Wrapf(err, "failed to check file presence in cache")
					}

					if err := e.writeLayer(groupCtx, cm, blob, dgstPair); err != nil {
						return err
					}
				}

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

	layerSet := make([]*cacheservice.CacheLayer, 0, len(cacheConfig.Layers))
	for _, layer := range cacheConfig.Layers {
		l := &cacheservice.CacheLayer{
			BlobDigest: string(layer.Blob),
			MediaType:  layer.Annotations.MediaType,
			DiffId:     string(layer.Annotations.DiffID),
			Size:       uint64(layer.Annotations.Size),
		}

		if layer.ParentIndex >= 0 {
			l.ParentBlobDigest = string(cacheConfig.Layers[layer.ParentIndex].Blob)
		}
		if createdAt := layer.Annotations.CreatedAt; createdAt.IsZero() {
			l.CreatedAt = createdAt.Unix()
		}
		layerSet = append(layerSet, l)
	}

	req := &cacheservice.ImportRequest{}
	for _, record := range cacheConfig.Records {
		r := &cacheservice.ExportableCacheRecord{
			Digest: string(record.Digest),
		}
		for _, res := range record.Results {
			l := layerSet[res.LayerIndex]
			r.Layers = append(r.Layers, l)
		}

		for _, inputs := range record.Inputs {
			inputSet := &cacheservice.CacheRecordInputs{}
			for _, inp := range inputs {
				inputSet.Inputs = append(inputSet.Inputs, &cacheservice.CacheRecordInput{
					LinkIndex: int64(inp.LinkIndex),
					Selector:  inp.Selector,
				})
			}
			r.Inputs = append(r.Inputs, inputSet)
		}
		req.Records = append(req.Records, r)
	}

	cacheClient := cacheservice.NewCacheManagerClient(e.cc)
	resp, err := cacheClient.Import(ctx, req)
	if err != nil {
		return nil, err
	}

	var attrs map[string]string
	if len(resp.Attrs) > 0 {
		attrs = make(map[string]string, len(resp.Attrs))
		for _, kv := range resp.Attrs {
			attrs[kv.Key] = kv.Value
		}
	}
	return attrs, nil
}

func (e *exporter) writeLayer(ctx context.Context, client cacheservice.CacheManagerClient, blob digest.Digest, dgstPair v1.DescriptorProviderPair) (retErr error) {
	layerDone := progress.OneOff(ctx, fmt.Sprintf("writing layer %s", blob))
	defer func() { layerDone(retErr) }()

	ra, err := dgstPair.Provider.ReaderAt(ctx, dgstPair.Descriptor)
	if err != nil {
		return layerDone(errors.Wrap(err, "error reading layer blob from provider"))
	}
	defer ra.Close()

	ingester, err := client.LayerUpload(ctx)
	if err != nil {
		return err
	}

	p := make([]byte, 4096)
	reader := newSectionReader(ra, ra.Size())
	for {
		n, err := reader.Read(p)
		if err != nil && !errors.Is(err, io.EOF) {
			ingester.CloseAndRecv()
			return err
		}

		if n > 0 {
			if err := ingester.Send(&cacheservice.BytesMessage{
				Data: p[:n],
			}); err != nil {
				ingester.CloseAndRecv()
				return err
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	upload, err := ingester.CloseAndRecv()
	if err != nil {
		return err
	}

	req := &cacheservice.LayerCommitRequest{
		Id:     upload.Id,
		Digest: string(blob),
	}

	_, err = client.LayerCommit(ctx, req)
	return err
}

type nopCloserSectionReader struct {
	*io.SectionReader
}

func newSectionReader(ra content.ReaderAt, n int64) io.Reader {
	return &nopCloserSectionReader{io.NewSectionReader(ra, 0, n)}
}

func (*nopCloserSectionReader) Close() error { return nil }

func outputKey(dgst digest.Digest, idx int) digest.Digest {
	return digest.FromBytes(fmt.Appendf(nil, "%s@%d", dgst, idx))
}
