package netproxyprovider

import (
	"context"

	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/session/netproxy"
	"google.golang.org/grpc"
)

func NewNetworkProxyProvider() session.Attachable {
	return &networkProxyProvider{}
}

type networkProxyProvider struct{}

func (np *networkProxyProvider) Register(server *grpc.Server) {
	netproxy.RegisterNetworkProxyServer(server, np)
}

func (np *networkProxyProvider) Dial(ctx context.Context, req *netproxy.DialRequest) (*netproxy.DialResponse, error) {
	panic("implement me")
}

func (np *networkProxyProvider) Connect(stream grpc.BidiStreamingServer[netproxy.NetworkPacket, netproxy.NetworkPacket]) error {
	panic("implement me")
}
