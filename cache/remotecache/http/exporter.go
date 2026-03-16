package http

import (
	"context"
	"errors"

	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
)

// ResolveCacheExporterFunc for "local" cache exporter.
func ResolveCacheExporterFunc(sm *session.Manager) remotecache.ResolveCacheExporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
		return nil, errors.New("implement me")
	}
}

// type exporter struct {
// 	remotecache.Exporter
// }
//
// func (*exporter) Name() string {
// 	return "exporting cache to http cache"
// }
