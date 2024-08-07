package bboltcachestorage

import (
	"bytes"
	"encoding/json"
	"sync"
	"time"

	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/db"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

const (
	flushInterval = 200 * time.Millisecond
	maxSize       = 1024
)

var ErrClosed = errors.Errorf("closed")

type WAL struct {
	db db.DB

	mu          sync.Mutex
	linkEntries []walLinkEntry
	results     []walResultEntry
	closed      bool

	done     chan struct{}
	doneOnce sync.Once

	wg     errgroup.Group
	ticker *time.Ticker
}

func NewWAL(db db.DB) *WAL {
	w := &WAL{db: db}
	w.start()
	return w
}

func (w *WAL) AddLink(id string, link solver.CacheInfoLink, target string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	w.linkEntries = append(w.linkEntries, walLinkEntry{
		ID:     id,
		Link:   link,
		Target: target,
	})
	return w.flush(false)
}

func (w *WAL) AddResult(id string, res solver.CacheResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	w.results = append(w.results, walResultEntry{
		ID:     id,
		Result: res,
	})
	return w.flush(false)
}

func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.flush(true)
}

func (w *WAL) Close() error {
	w.doneOnce.Do(func() {
		close(w.done)
	})
	return w.wg.Wait()
}

func (w *WAL) start() {
	w.done = make(chan struct{})
	w.ticker = time.NewTicker(flushInterval)
	w.ticker.Stop()

	w.wg.Go(w.loop)
}

func (w *WAL) queueFlush() {
	w.ticker.Reset(flushInterval)
}

func (w *WAL) flush(force bool) error {
	if !force && !w.shouldFlush() {
		// Reset the flush interval since we're not performing
		// a flush but a change must have been made if we're invoking
		// this function.
		w.ticker.Reset(flushInterval)
		return nil
	}

	// Stop the ticker as we're going to clear the buffers.
	w.ticker.Stop()
	if err := w.db.Update(func(tx *bbolt.Tx) error {
		if err := w.addLinks(tx); err != nil {
			return err
		}
		return w.addResults(tx)
	}); err != nil {
		return err
	}

	w.linkEntries = w.linkEntries[:0]
	w.results = w.results[:0]
	return nil
}

func (w *WAL) shouldFlush() bool {
	numLinks, numResults := len(w.linkEntries), len(w.results)
	return numLinks+numResults >= maxSize || numLinks >= maxSize || numResults >= maxSize
}

func (w *WAL) loop() error {
	for {
		select {
		case <-w.ticker.C:
			if err := w.Flush(); err != nil {
				return err
			}
		case <-w.done:
			return w.close()
		}
	}
}

func (w *WAL) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.flush(true)
	w.closed = true
	return err
}

type walLinkEntry struct {
	ID     string
	Link   solver.CacheInfoLink
	Target string
}

func (w *WAL) addLinks(tx *bbolt.Tx) error {
	links := tx.Bucket([]byte(linksBucket))
	backlinks := tx.Bucket([]byte(backlinksBucket))
	for _, link := range w.linkEntries {
		b, err := links.CreateBucketIfNotExists([]byte(link.ID))
		if err != nil {
			return err
		}

		dt, err := json.Marshal(link.Link)
		if err != nil {
			return err
		}

		if err := b.Put(bytes.Join([][]byte{dt, []byte(link.Target)}, []byte("@")), []byte{}); err != nil {
			return err
		}

		b, err = backlinks.CreateBucketIfNotExists([]byte(link.Target))
		if err != nil {
			return err
		}

		if err := b.Put([]byte(link.ID), []byte{}); err != nil {
			return err
		}
	}
	return nil
}

type walResultEntry struct {
	ID     string
	Result solver.CacheResult
}

func (w *WAL) addResults(tx *bbolt.Tx) error {
	links := tx.Bucket([]byte(linksBucket))
	results := tx.Bucket([]byte(resultBucket))
	byResults := tx.Bucket([]byte(byResultBucket))

	for _, res := range w.results {
		if _, err := links.CreateBucketIfNotExists([]byte(res.ID)); err != nil {
			return err
		}

		b, err := results.CreateBucketIfNotExists([]byte(res.ID))
		if err != nil {
			return err
		}
		dt, err := json.Marshal(res.Result)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(res.Result.ID), dt); err != nil {
			return err
		}
		b, err = byResults.CreateBucketIfNotExists([]byte(res.Result.ID))
		if err != nil {
			return err
		}
		if err := b.Put([]byte(res.ID), []byte{}); err != nil {
			return err
		}
	}
	return nil
}
