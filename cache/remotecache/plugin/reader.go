package plugin

import (
	"context"
	"io"

	"github.com/containerd/containerd/v2/core/content"
	cacheservice "github.com/moby/buildkit/api/services/cache"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
)

func (cm *cacheManager) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
	dgst := desc.Digest.Encoded()
	open := func(offset int64) (_ io.ReadCloser, retErr error) {
		in := &cacheservice.LayerGetRequest{
			Digest: dgst,
			Offset: uint64(offset),
		}

		resp, err := cm.cs.Get(ctx, in)
		if err != nil {
			return nil, err
		}
		return newBytesReader(resp), nil
	}
	readerAtCloser := toReaderAtCloser(open)
	return &readerAt{ReaderAtCloser: readerAtCloser, size: desc.Size}, nil
}

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

type readerAtCloser struct {
	offset int64
	rc     io.ReadCloser
	ra     io.ReaderAt
	open   func(offset int64) (io.ReadCloser, error)
	closed bool
}

func toReaderAtCloser(open func(offset int64) (io.ReadCloser, error)) ReaderAtCloser {
	return &readerAtCloser{
		open: open,
	}
}

func (hrs *readerAtCloser) ReadAt(p []byte, off int64) (n int, err error) {
	if hrs.closed {
		return 0, io.EOF
	}

	if hrs.ra != nil {
		return hrs.ra.ReadAt(p, off)
	}

	if hrs.rc == nil || off != hrs.offset {
		if hrs.rc != nil {
			hrs.rc.Close()
			hrs.rc = nil
		}
		rc, err := hrs.open(off)
		if err != nil {
			return 0, err
		}
		hrs.rc = rc
	}
	if ra, ok := hrs.rc.(io.ReaderAt); ok {
		hrs.ra = ra
		n, err = ra.ReadAt(p, off)
	} else {
		for {
			var nn int
			nn, err = hrs.rc.Read(p)
			n += nn
			p = p[nn:]
			if nn == len(p) || err != nil {
				break
			}
		}
	}

	hrs.offset += int64(n)
	return
}

func (hrs *readerAtCloser) Close() error {
	if hrs.closed {
		return nil
	}
	hrs.closed = true
	if hrs.rc != nil {
		return hrs.rc.Close()
	}

	return nil
}

type readerAt struct {
	ReaderAtCloser
	size int64
}

func (r *readerAt) Size() int64 {
	return r.size
}

type bytesReader struct {
	client grpc.ServerStreamingClient[cacheservice.BytesMessage]
	p      []byte
}

func newBytesReader(client grpc.ServerStreamingClient[cacheservice.BytesMessage]) io.ReadCloser {
	return &bytesReader{client: client}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		n += copy(p[n:], r.p)
		r.p = r.p[n:]

		// We read the entire buffer so feel free to return.
		if len(p) == n {
			break
		}

		msg, err := r.client.Recv()
		if err != nil {
			return n, err
		}
		r.p = msg.Data
	}
	return n, nil
}

func (r *bytesReader) Close() error {
	return r.client.CloseSend()
}
