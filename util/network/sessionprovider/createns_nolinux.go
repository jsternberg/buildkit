//go:build !linux

package sessionprovider

import (
	"github.com/pkg/errors"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

func createNetNS(s *sessionProvider, id string) (stack.LinkEndpoint, string, error) {
	return nil, "", errors.New("network namespaces are not supported on this platform")
}

func deleteNetNS(nsPath string) error {
	return nil
}
