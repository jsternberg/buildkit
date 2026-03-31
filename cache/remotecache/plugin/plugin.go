package plugin

import (
	"context"
	"strconv"

	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/worker"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"
)

const (
	pkgpath               = "github.com/moby/buildkit/cache/remotecache/http"
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
