package sqlcachestorage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/cachedigest"
	digest "github.com/opencontainers/go-digest"
)

type Store struct {
	db      *sql.DB
	results solver.CacheResultStorage
}

func NewStore(path string, results solver.CacheResultStorage) (*Store, error) {
	db, err := sqliteOpen(path)
	if err != nil {
		return nil, err
	}

	s := &Store{
		db:      db,
		results: results,
	}
	if err := s.autoMigrate(); err != nil {
		_ = s.db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) autoMigrate() error {
	for _, stmt := range []string{
		CREATE_TABLE_RECORDS_SQL,
		CREATE_TABLE_LINKS_SQL,
		CREATE_INDEX_LINKS_SOURCE_RECORD_SQL,
		CREATE_INDEX_RECORDS_ID_SQL,
		CREATE_INDEX_RECORDS_ID_AND_RECORD_SQL,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Query(deps []solver.CacheKeyWithSelector, input solver.Index, dgst digest.Digest, output solver.Index) ([]*solver.CacheKey, error) {
	id := rootKey(dgst, output)
	if len(deps) == 0 {
		var exists int
		if err := s.db.QueryRow(LINK_EXISTS_SQL, id).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			return nil, err
		}
		return []*solver.CacheKey{{ID: id.String()}}, nil
	}

	ids := make(map[string]struct{})
	args := []any{nil, id, int(input), nil}
	// TODO: can optimize this sql query so there's only one query.
	for _, dep := range deps {
		args[0] = dep.CacheKey.ID

		var (
			rows *sql.Rows
			err  error
		)
		if dep.Selector != "" {
			args[3] = dep.Selector
			rows, err = s.db.Query(QUERY_LINKS_WITH_SELECTOR_SQL, args...)
		} else {
			rows, err = s.db.Query(QUERY_LINKS_SQL, args[:3]...)
		}

		if err != nil {
			return nil, err
		}

		var id string
		for rows.Next() {
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids[id] = struct{}{}
		}
		rows.Close()
	}

	if len(ids) == 0 {
		return nil, nil
	}

	keys := make([]*solver.CacheKey, 0, len(ids))
	for id := range ids {
		keys = append(keys, &solver.CacheKey{ID: id})
	}
	return keys, nil
}

func (s *Store) Records(ctx context.Context, ck *solver.CacheKey) ([]*solver.CacheRecord, error) {
	rows, err := s.db.QueryContext(ctx, SELECT_RECORDS_SQL, ck.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]*solver.CacheRecord, 0)
	for rows.Next() {
		var record solver.CacheRecord
		if err := rows.Scan(&record.ID, &record.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, &record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Store) Load(ctx context.Context, key *solver.CacheKey, id string) (solver.Result, error) {
	res := solver.CacheResult{ID: id}
	if err := s.db.QueryRowContext(ctx, SELECT_RECORD_SQL, key.ID, res.ID).Scan(&res.CreatedAt); err != nil {
		return nil, err
	}
	return s.results.Load(ctx, res)
}

func (s *Store) Save(k *solver.CacheKey, r solver.Result, createdAt time.Time) (_ *solver.CacheRecord, retErr error) {
	res, err := s.results.Save(r, createdAt)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.Exec(INSERT_RECORD_SQL, k.ID, res.CreatedAt, res.ID); err != nil {
		return nil, err
	}

	type link struct {
		Source *solver.CacheKey
		Link   solver.CacheInfoLink
		Target string
	}

	getLinks := func(k *solver.CacheKey) (links []link) {
		for i, deps := range k.Deps() {
			for _, ck := range deps {
				l := solver.CacheInfoLink{
					Input:    solver.Index(i),
					Output:   k.Output(),
					Digest:   k.Digest(),
					Selector: ck.Selector,
				}
				links = append(links, link{
					Source: ck.CacheKey.CacheKey,
					Link:   l,
					Target: k.ID,
				})
			}
		}
		return links
	}

	for pending := getLinks(k); len(pending) > 0; pending = pending[1:] {
		l := pending[0]
		// TODO: check whether the link already exists before inserting to avoid
		// duplicate rows. The SQL schema does not currently expose a HasLink
		// helper analogous to the kv backend.
		dgst := rootKey(l.Link.Digest, l.Link.Output).String()
		var selector any = nil
		if l.Link.Selector != "" {
			selector = l.Link.Selector
		}
		if _, err := tx.Exec(INSERT_LINK_SQL, l.Source.ID, dgst, int(l.Link.Input), l.Target, selector); err != nil {
			return nil, err
		}
		pending = append(pending, getLinks(l.Source)...)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &solver.CacheRecord{
		ID:        res.ID,
		CreatedAt: res.CreatedAt,
	}, nil
}

func (s *Store) ReleaseUnreferenced(ctx context.Context) error {
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func rootKey(dgst digest.Digest, output solver.Index) digest.Digest {
	out, _ := cachedigest.FromBytes(fmt.Appendf(nil, "%s@%d", dgst, output), cachedigest.TypeString)
	if strings.HasPrefix(dgst.String(), "random:") {
		return digest.Digest("random:" + dgst.Encoded())
	}
	return out
}
