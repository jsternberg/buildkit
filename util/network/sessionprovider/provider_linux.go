package sessionprovider

import (
	"net"
	"syscall"

	"github.com/pkg/errors"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
)

func setupRoutes(nsPath string) error {
	ns, err := netns.GetFromPath(nsPath)
	if err != nil {
		return errors.Wrap(err, "failed to get network namespace")
	}
	defer ns.Close()

	hdl, err := netlink.NewHandleAt(ns)
	if err != nil {
		return errors.Wrap(err, "failed to create netlink handle")
	}
	defer hdl.Close()

	// Get the tun0 interface
	tun, err := hdl.LinkByName("tun0")
	if err != nil {
		return errors.Wrap(err, "failed to get tun0 interface")
	}

	// Bring up the tun0 interface
	if err := hdl.LinkSetUp(tun); err != nil {
		return errors.Wrap(err, "failed to bring up tun0")
	}

	// Assign an IP address to the tunnel interface
	addr, err := netlink.ParseAddr("192.168.1.2/24")
	if err != nil {
		return errors.Wrap(err, "failed to parse address")
	}

	if err := hdl.AddrAdd(tun, addr); err != nil && !errors.Is(err, syscall.EEXIST) {
		return errors.Wrap(err, "failed to add address to tun0")
	}

	// Parse the default route destination (0.0.0.0/0 for IPv4)
	_, dst, err := net.ParseCIDR("0.0.0.0/0")
	if err != nil {
		return errors.Wrap(err, "failed to parse default route CIDR")
	}

	// Add a default route that forwards all traffic through tun0
	route := &netlink.Route{
		LinkIndex: tun.Attrs().Index,
		Dst:       dst,
	}
	if err := hdl.RouteAdd(route); err != nil && !errors.Is(err, syscall.EEXIST) {
		return errors.Wrap(err, "failed to add default route")
	}

	return nil
}
