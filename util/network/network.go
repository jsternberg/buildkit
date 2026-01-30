package network

import (
	"context"
	"io"

	resourcestypes "github.com/moby/buildkit/executor/resources/types"
	"github.com/moby/buildkit/session"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// Provider interface for Network
type Provider interface {
	io.Closer
	New(ctx context.Context, hostname string, g session.Group) (Namespace, error)
}

// Namespace of network for workers
type Namespace interface {
	io.Closer
	// Set the namespace on the spec
	Set(*specs.Spec) error

	Sample() (*resourcestypes.NetworkSample, error)
}
