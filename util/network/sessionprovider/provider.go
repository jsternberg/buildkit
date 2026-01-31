package sessionprovider

import (
	"context"

	resourcestypes "github.com/moby/buildkit/executor/resources/types"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/network"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/rawfile"
	"gvisor.dev/gvisor/pkg/tcpip/link/tun"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
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

func newNetworkStack(addr netip.Address, ep stack.LinkEndpoint, client NetworkProxyClient) (*stack.Stack, error) {
	// basic network stack creation. notice that we don't support icmp
	// so no pinging and we only support tcp for now. I think we could feasibly
	// use this to also support udp, but I don't view that as very important
	// at the current moment.
	//
	// might need to include udp though as dns seems to use udp by default.
	// we can configure dns to use tcp though according to google.
	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})
	
	forwarder := tcp.NewForwarder(s, 0, 1024, func(req *tcp.ForwarderRequest) {
		// it's called local address and port, but it's our destination
		// address hence the remote from our perspective.
		remote, err := dialTCP(client, req.ID().LocalAddress, req.ID().LocalPort)
		if err != nil {
			// log error and send the reset since we couldn't dial.
			req.Complete(true)
			return
		}
		
		var wq waiter.Queue
		ep, e := req.CreateEndpoint(&wq)
		if e != nil {
			// log error and send the reset. we use a different name
			// because create endpoint returns a tcpip.Error instead of error.
			req.Complete(true)
			return
		}
		
		// nothing else fails at this point so send the complete.
		// this is the connection to our own network stack and the remote address.
		// it's local relative to us.
		local := gonet.NewTCPConn(&wq, ep)
		defer local.Close()
		
		// two goroutines. one side reads from local and writes to remote,
		// the other reads from remote and writes to local.
		streamConn(local, remote)
	})
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, forwarder.HandlePacket)
	
	if err := s.CreateNIC(1, ep); err != nil {
		return err
	}
	
	// this allows our nic to handle ip addresses it doesn't own
	// which allows the tcp forwarder to function correctly.
	s.SetPromiscuousMode(1, true)
	s.SetSpoofing(1, true)
	
	// not sure if this part is needed, but the address should match
	// the address configured for the tunnel.
	protocolAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: addr.WithPrefix(),
	}
	if err := s.AddProtocolAddress(1, protocolAddr, stack.AddressProperties{}); err != nil {
		panic(err)
	}
	
	// send all traffic to the link endpoint for the grpc session.
	// likely need both ipv4 and ipv6 but I've only tested ipv4 so far.
	s.SetRouteTable([]tcpip.Route{
		{
			Destination: header.IPv4EmptySubnet,
			NIC:         1,
		},
	}
	return s, nil
}

// ip tuntap add name tun0 mode tun
// ip link set tun0 up
// ip addr add 192.168.1.2/24 dev tun0
