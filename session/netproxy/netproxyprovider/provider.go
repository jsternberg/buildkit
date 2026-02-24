package netproxyprovider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session/netproxy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NetworkProxy implements the NetworkProxyServer interface.
type NetworkProxy struct {
	conns map[string]*proxyConn
	mu    sync.Mutex
}

func New() *NetworkProxy {
	return &NetworkProxy{
		conns: make(map[string]*proxyConn),
	}
}

func (p *NetworkProxy) Register(server *grpc.Server) {
	netproxy.RegisterNetworkProxyServer(server, p)
}

func (p *NetworkProxy) Dial(ctx context.Context, req *netproxy.DialRequest) (*netproxy.DialResponse, error) {
	conn, err := p.dial(ctx, req)
	if err != nil {
		return nil, err
	}

	// Generate a unique ID for this connection
	id := identity.NewID()

	mtu := req.Mtu
	if mtu == 0 {
		mtu = 1500
	}

	// Store the connection in the map
	p.mu.Lock()
	p.conns[id] = &proxyConn{
		Conn: conn,
		mtu:  mtu,
	}
	p.mu.Unlock()

	return &netproxy.DialResponse{
		Id: id,
	}, nil
}

func (p *NetworkProxy) dial(ctx context.Context, req *netproxy.DialRequest) (net.Conn, error) {
	fmt.Printf("dialing %s %s\n", req.Addr, req.Protocol)
	switch req.Protocol {
	case netproxy.Protocol_TCP:
		return dialTCP(ctx, req)
	case netproxy.Protocol_UDP:
		return dialUDP(ctx, req)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported protocol: %v", req.Protocol)
	}
}

func (p *NetworkProxy) Connect(stream grpc.BidiStreamingServer[netproxy.NetworkPacket, netproxy.NetworkPacket]) error {
	// Read the first message which should be an InitMessage
	fmt.Println("received connect attempt")
	packet, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive init message: %v", err)
	}

	// Extract the InitMessage
	initMsg, ok := packet.Packet.(*netproxy.NetworkPacket_Init)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "first message must be an init message")
	}

	id := initMsg.Init.Id
	if id == "" {
		return status.Errorf(codes.InvalidArgument, "connection id is required")
	}
	fmt.Printf("connection id is %s\n", id)

	// Look up and remove the connection from the map
	p.mu.Lock()
	conn, exists := p.conns[id]
	if exists {
		delete(p.conns, id)
	}
	p.mu.Unlock()

	if !exists {
		return status.Errorf(codes.NotFound, "connection %s not found", id)
	}

	// Ensure the connection is closed when we're done
	defer conn.Close()

	// Create an errgroup for managing goroutines
	eg, ctx := errgroup.WithContext(stream.Context())

	// Goroutine 1: Read from stream and write to connection
	eg.Go(func() error {
		for {
			packet, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}

			// Extract the data message
			dataMsg, ok := packet.Packet.(*netproxy.NetworkPacket_Data)
			if !ok {
				return status.Errorf(codes.InvalidArgument, "expected data message")
			}

			fmt.Printf("received data packet of size %d\n", len(dataMsg.Data.Data))

			// Write to the connection
			if _, err := conn.Write(dataMsg.Data.Data); err != nil {
				return err
			}
		}
	})

	// Goroutine 2: Read from connection and write to stream
	eg.Go(func() error {
		fmt.Printf("connection mtu %d\n", conn.mtu)
		buf := make([]byte, conn.mtu)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			n, err := conn.Read(buf)
			fmt.Printf("read %d bytes from connection\n", n)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}

			// Send the data back to the stream
			packet := &netproxy.NetworkPacket{
				Packet: &netproxy.NetworkPacket_Data{
					Data: &netproxy.BytesMessage{
						Data: buf[:n],
					},
				},
			}
			if err := stream.Send(packet); err != nil {
				return err
			}
		}
	})

	// Wait for both goroutines to complete
	return eg.Wait()
}

func dialTCP(ctx context.Context, req *netproxy.DialRequest) (net.Conn, error) {
	if req.Addr == nil {
		return nil, status.Errorf(codes.InvalidArgument, "address is required for TCP")
	}

	addr := fmt.Sprintf("%s:%d", req.Addr.Ip, req.Addr.Port)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to dial %s: %v", addr, err)
	}

	fmt.Println("successfully dialed location")
	return conn, nil
}

func dialUDP(ctx context.Context, req *netproxy.DialRequest) (net.Conn, error) {
	var addr string
	if req.Addr != nil {
		addr = fmt.Sprintf("%s:%d", req.Addr.Ip, req.Addr.Port)
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to dial %s: %v", addr, err)
	}

	return conn, nil
}

type proxyConn struct {
	net.Conn
	mtu uint32
}
