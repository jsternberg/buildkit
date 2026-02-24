package sessionprovider

import (
	"context"
	"io"
	"net/netip"

	"github.com/containerd/containerd/v2/pkg/oci"
	resourcestypes "github.com/moby/buildkit/executor/resources/types"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/session/netproxy"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/network"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

type Opt struct {
	Root           string
	SessionManager *session.Manager
}

func New(opt Opt) network.Provider {
	addr := netip.MustParseAddr("192.168.1.2")
	return &sessionProvider{
		root: opt.Root,
		addr: addr,
		sm:   opt.SessionManager,
	}
}

type sessionProvider struct {
	root string
	addr netip.Addr
	sm   *session.Manager
}

func (s *sessionProvider) New(ctx context.Context, hostname string, g session.Group) (network.Namespace, error) {
	if g == nil {
		return nil, errors.New("no session provided")
	}

	var caller session.Caller
	if err := s.sm.Any(ctx, g, func(_ context.Context, _ string, c session.Caller) error {
		caller = c
		return nil
	}); err != nil {
		return nil, err
	}

	return s.newSessionNS(ctx, caller)
}

func (s *sessionProvider) newSessionNS(ctx context.Context, caller session.Caller) (_ *sessionNS, retErr error) {
	id := identity.NewID()
	trace.SpanFromContext(ctx).AddEvent("creating new network namespace with session tunnel")
	bklog.G(ctx).Debugf("creating new network namespace %s with session tunnel", id)
	ep, nsPath, err := createNetNS(s, id)
	if err != nil {
		return nil, err
	}
	trace.SpanFromContext(ctx).AddEvent("finished creating network namespace with session tunnel")
	bklog.G(ctx).Debugf("finished creating network namespace %s with session tunnel", id)

	defer func() {
		if retErr != nil {
			deleteNetNS(nsPath)
		}
	}()

	if err := setupRoutes(nsPath); err != nil {
		return nil, err
	}

	trace.SpanFromContext(ctx).AddEvent("creating network stack")
	bklog.G(ctx).Debugf("creating network stack")

	client := netproxy.NewNetworkProxyClient(caller.Conn())
	stack, tcpErr := newNetworkStack(s.addr, ep, client)
	if tcpErr != nil {
		return nil, errors.Errorf("network stack creation error: %s", tcpErr)
	}

	trace.SpanFromContext(ctx).AddEvent("finished creating network stack")
	bklog.G(ctx).Debugf("finished creating network stack")

	return &sessionNS{
		ep:     ep,
		nsPath: nsPath,
		stack:  stack,
	}, nil
}

func (s *sessionProvider) Close() error {
	return nil
}

type sessionNS struct {
	ep     stack.LinkEndpoint
	nsPath string
	stack  *stack.Stack
}

func (s *sessionNS) Set(spec *specs.Spec) error {
	return oci.WithLinuxNamespace(specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: s.nsPath,
	})(nil, nil, nil, spec)
}

func (s *sessionNS) Close() error {
	s.stack.Close()
	return deleteNetNS(s.nsPath)
}

func (s *sessionNS) Sample() (*resourcestypes.NetworkSample, error) {
	return nil, nil
}

func newNetworkStack(addr netip.Addr, ep stack.LinkEndpoint, client netproxy.NetworkProxyClient) (*stack.Stack, tcpip.Error) {
	// basic network stack creation. notice that we don't support icmp
	// so no pinging and we only support tcp for now. I think we could feasibly
	// use this to also support udp, but I don't view that as very important
	// at the current moment.
	//
	// might need to include udp though as dns seems to use udp by default.
	// we can configure dns to use tcp though according to google.
	s := stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocolWithOptions(ipv4.Options{
				AllowExternalLoopbackTraffic: true,
			}),
		},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})

	forwarder := tcp.NewForwarder(s, 0, 1024, func(req *tcp.ForwarderRequest) {
		// it's called local address and port, but it's our destination
		// address hence the remote from our perspective.
		bklog.G(context.Background()).Debugf("forwarding %s:%d", req.ID().LocalAddress, req.ID().LocalPort)

		addr := tcpip.AddrFromSlice(netip.MustParseAddr("127.0.0.1").AsSlice())
		remote, err := dialTCP(client, addr, req.ID().LocalPort)
		if err != nil {
			// log error and send the reset since we couldn't dial.
			req.Complete(true)
			bklog.G(context.Background()).Debugf("error dialing tcp: %s", err)
			return
		}
		bklog.G(context.Background()).Debugf("dial successful")

		var wq waiter.Queue
		ep, e := req.CreateEndpoint(&wq)
		if e != nil {
			// log error and send the reset. we use a different name
			// because create endpoint returns a tcpip.Error instead of error.
			req.Complete(true)
			bklog.G(context.Background()).Debugf("error creating request endpoint: %s", e)
			return
		}
		bklog.G(context.Background()).Debug("endpoint created")
		req.Complete(false)
		bklog.G(context.Background()).Debug("connection established")

		// nothing else fails at this point so send the complete.
		// this is the connection to our own network stack and the remote address.
		// it's local relative to us.
		local := gonet.NewTCPConn(&wq, ep)
		defer local.Close()

		// two goroutines. one side reads from local and writes to remote,
		// the other reads from remote and writes to local.
		bklog.G(context.Background()).Debug("streaming between local and remote")
		streamConn(local, remote)
	})
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, forwarder.HandlePacket)

	if err := s.CreateNIC(1, ep); err != nil {
		return nil, err
	}

	// this allows our nic to handle ip addresses it doesn't own
	// which allows the tcp forwarder to function correctly.
	s.SetPromiscuousMode(1, true)
	s.SetSpoofing(1, true)

	// not sure if this part is needed, but the address should match
	// the address configured for the tunnel.
	protocolAddr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFromSlice(addr.AsSlice()).WithPrefix(),
	}
	if err := s.AddProtocolAddress(1, protocolAddr, stack.AddressProperties{}); err != nil {
		return nil, err
	}

	// send all traffic to the link endpoint for the grpc session.
	// likely need both ipv4 and ipv6 but I've only tested ipv4 so far.
	s.SetRouteTable([]tcpip.Route{
		{
			Destination: header.IPv4EmptySubnet,
			NIC:         1,
		},
	})
	return s, nil
}

func dialTCP(client netproxy.NetworkProxyClient, addr tcpip.Address, port uint16) (grpc.BidiStreamingClient[netproxy.NetworkPacket, netproxy.NetworkPacket], error) {
	ctx := context.Background()

	req := &netproxy.DialRequest{
		Protocol: netproxy.Protocol_TCP,
		Addr: &netproxy.Address{
			Ip:   addr.String(),
			Port: uint32(port),
		},
		Mtu: 1500,
	}

	resp, err := client.Dial(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial")
	}
	bklog.G(context.Background()).Debugf("client dialed with id: %s", resp.Id)

	stream, err := client.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}

	bklog.G(context.Background()).Debug("connecting to network proxy")
	// Send init message with the connection ID
	initPacket := &netproxy.NetworkPacket{
		Packet: &netproxy.NetworkPacket_Init{
			Init: &netproxy.InitMessage{
				Id: resp.Id,
			},
		},
	}

	if err := stream.Send(initPacket); err != nil {
		return nil, errors.Wrap(err, "failed to send init message")
	}
	bklog.G(context.Background()).Debug("initialized connection")

	return stream, nil
}

func streamConn(local *gonet.TCPConn, remote grpc.BidiStreamingClient[netproxy.NetworkPacket, netproxy.NetworkPacket]) {
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() (retErr error) {
		defer func() {
			if retErr != nil {
				bklog.G(context.Background()).Debugf("error in send loop: %s", retErr)
			}
		}()
		defer remote.CloseSend()

		// read from the local, send to the remote.
		// todo: use the mtu from the physical tunnel
		buf := make([]byte, 1500)
		for {
			n, err := local.Read(buf)
			if err != nil {
				return nonEOFError(err)
			}

			pkt := &netproxy.NetworkPacket{
				Packet: &netproxy.NetworkPacket_Data{
					Data: &netproxy.BytesMessage{
						Data: buf[:n],
					},
				},
			}
			bklog.G(context.Background()).Debugf("sending packet of size %d", n)
			if err := remote.Send(pkt); err != nil {
				return err
			}
		}
	})

	eg.Go(func() (retErr error) {
		// read from the remote and send to the local.
		defer func() {
			if retErr != nil {
				bklog.G(context.Background()).Debugf("error in write loop: %s", retErr)
			}
		}()
		defer local.CloseWrite()

		for {
			pkt, err := remote.Recv()
			if err != nil {
				return nonEOFError(err)
			}

			if data, ok := pkt.Packet.(*netproxy.NetworkPacket_Data); ok {
				if _, err := local.Write(data.Data.Data); err != nil {
					return err
				}
			} else {
				return errors.Errorf("unexpected packet type: %T", pkt.Packet)
			}
		}
	})

	if err := eg.Wait(); err != nil {
		// todo: log a message or somehow send it back to the client or something.
		bklog.G(context.Background()).Debugf("error while streaming: %s", err)
		return
	}
}

func nonEOFError(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
