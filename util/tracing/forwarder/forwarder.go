package forwarder

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	errUnstarted      = errors.New("not started")
	errAlreadyStarted = errors.New("already started")
	errShutdown       = errors.New("shutdown")
)

type Exporter struct {
	exp sdktrace.SpanExporter

	ctx      context.Context
	cancel   context.CancelCauseFunc
	wg       sync.WaitGroup
	shutdown bool
	mu       sync.RWMutex

	startOnce sync.Once
	stopOnce  sync.Once
}

// New constructs a new Exporter and starts it.
func New(ctx context.Context, exp sdktrace.SpanExporter) (*Exporter, error) {
	e := NewUnstarted(exp)
	if err := e.Start(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

// NewUnstarted constructs a new Exporter and does not start it.
func NewUnstarted(exp sdktrace.SpanExporter) *Exporter {
	return &Exporter{exp: exp}
}

// Start marks the Exporter as started.
func (e *Exporter) Start(ctx context.Context) error {
	err := errAlreadyStarted
	e.startOnce.Do(func() {
		e.mu.Lock()
		defer e.mu.Unlock()

		e.ctx, e.cancel = context.WithCancelCause(context.Background())
		err = nil
	})
	return err
}

func (e *Exporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ctx == nil {
		return errUnstarted
	} else if e.shutdown {
		return errShutdown
	}

	e.wg.Go(func() {
		e.exportSpans(spans)
	})
	return nil
}

func (e *Exporter) exportSpans(spans []sdktrace.ReadOnlySpan) {
	if err := e.exp.ExportSpans(e.ctx, spans); err != nil {
		otel.Handle(err)
	}
}

func (e *Exporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	cancel := e.cancel
	e.mu.Unlock()

	if cancel == nil {
		return nil
	}

	var err error
	e.stopOnce.Do(func() {
		// We have marked the exporter as shut down so try
		// to wait for any pending exports.
		var wg sync.WaitGroup
		done := make(chan struct{})

		wg.Go(func() {
			select {
			// All pending exports have finished.
			case <-done:
				// The shutdown deadline has been reached.
			case <-ctx.Done():
			}

			// Either all exports have completed or we've hit
			// our limit for the shutdown context. Cancel the
			// context either way.
			cause := context.Cause(ctx)
			if cause == nil {
				cause = context.Canceled
			}
			cancel(cause)
		})

		// Wait for the exports to complete and then mark the exporter
		// as done when it is.
		e.wg.Wait()
		close(done)

		// Ensure we don't leave orphaned goroutines.
		wg.Wait()

		// Finally finish by calling shutdown on the underlying exporter.
		// If the context already got canceled, this will probably
		// do nothing but just going with it anyway.
		err = e.exp.Shutdown(ctx)
	})
	return err
}

var _ sdktrace.SpanExporter = (*Exporter)(nil)
