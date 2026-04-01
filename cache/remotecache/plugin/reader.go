package plugin

import (
	"context"

	"github.com/containerd/containerd/v2/core/content"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

func (cm *cacheManager) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
	panic("implement me")
}
