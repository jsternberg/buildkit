package sessionprovider

import (
	"context"

	resourcestypes "github.com/moby/buildkit/executor/resources/types"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/network"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
)

func New(sm *session.Manager) network.Provider {
	return nil
}

type sessionProvider struct {
	sm *session.Manager
}

func (s *sessionProvider) New(_ context.Context, hostname string, g session.Group) (network.Namespace, error) {
	if g == nil {
		return nil, errors.New("no session provided")
	}
	return nil, nil
}

func (s *sessionProvider) Close() error {
	return nil
}

type sessionNS struct{}

func (s *sessionNS) Set(spec *specs.Spec) error {
	return nil
}

func (s *sessionNS) Close() error {
	return nil
}

func (s *sessionNS) Sample() (*resourcestypes.NetworkSample, error) {
	return nil, nil
}
