package sessionprovider

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/pkg/errors"
	"gvisor.dev/gvisor/pkg/rawfile"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/tcpip/link/tun"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

func openLinkEndpoint() (stack.LinkEndpoint, error) {
	const tunName = "tun0"

	// this will fail if the tunnel doesn't already exist.
	// you can swap this with the open call and open will
	// create the tunnel.
	mtu, err := rawfile.GetMTU(tunName)
	if err != nil {
		return nil, err
	}

	// this will create the tunnel if it doesn't exist or will
	// open the existing one.
	fd, err := tun.Open(tunName)
	if err != nil {
		return nil, err
	}

	return fdbased.New(&fdbased.Options{
		FDs: []int{fd},
		MTU: mtu,
	})
}

// ip tuntap add name tun0 mode tun
// ip link set tun0 up
// ip addr add 192.168.1.2/24 dev tun0

// unshareAndMount needs to be called in a separate thread
func unshareAndMountNetNS(target string, setup func() error) error {
	if err := syscall.Unshare(syscall.CLONE_NEWNET); err != nil {
		return err
	}

	if setup != nil {
		if err := setup(); err != nil {
			return err
		}
	}

	return syscall.Mount(fmt.Sprintf("/proc/self/task/%d/ns/net", syscall.Gettid()), target, "", syscall.MS_BIND, "")
}

func createNetNS(s *sessionProvider, id string) (ep stack.LinkEndpoint, _ string, err error) {
	nsPath := filepath.Join(s.root, "net/session", id)
	if err := os.MkdirAll(filepath.Dir(nsPath), 0o700); err != nil {
		return nil, "", err
	}

	f, err := os.Create(nsPath)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if err != nil {
			deleteNetNS(nsPath)
		}
	}()
	if err := f.Close(); err != nil {
		return nil, "", err
	}

	errCh := make(chan error)

	setupTun := func() (err error) {
		const tunName = "tun0"

		fd, err := tun.Open(tunName)
		if err != nil {
			return err
		}

		mtu, err := rawfile.GetMTU(tunName)
		if err != nil {
			return err
		}

		ep, err = fdbased.New(&fdbased.Options{
			FDs: []int{fd},
			MTU: mtu,
		})
		return err
	}

	go func() {
		defer close(errCh)
		runtime.LockOSThread()

		if err := unshareAndMountNetNS(nsPath, setupTun); err != nil {
			errCh <- err
		}

		// we leave the thread locked so go runtime terminates the thread
	}()

	if err := <-errCh; err != nil {
		return nil, "", err
	}
	return ep, nsPath, nil
}

func deleteNetNS(nsPath string) error {
	if err := os.Remove(nsPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrapf(err, "error removing network namespace %s", nsPath)
	}
	return nil
}
