package sqlcachestorage

import (
	"database/sql"
	"strings"

	"github.com/moby/buildkit/solver"
)

type ModifiableStore struct {
	tx  *sql.Tx
	err error
}

func (s *ModifiableStore) AddResult(id string, res solver.CacheResult) error {
	if err := s.addResult(id, res); err != nil {
		return s.setErr(err)
	}
	return nil
}

func (s *ModifiableStore) addResult(id string, res solver.CacheResult) error {
	_, err := s.tx.Exec(addResultSQL, id, res.ID, res.CreatedAt)
	return err
}

func (s *ModifiableStore) AddLink(id string, link solver.CacheInfoLink, target string) error {
	if err := s.addLink(id, link, target); err != nil {
		return s.setErr(err)
	}
	return nil
}

func (s *ModifiableStore) addLink(id string, link solver.CacheInfoLink, target string) error {
	_, err := s.tx.Exec(addLinkSQL, id, link.Input, link.Output, string(link.Digest), string(link.Selector), target)
	return err
}

func (s *ModifiableStore) Release(resultID string) error {
	if err := s.releaseBulk(resultID); err != nil {
		return s.setErr(err)
	}
	return s.DeleteUnreachableLinks()
}

func (s *ModifiableStore) ReleaseBulk(resultIDs []string) error {
	if len(resultIDs) == 0 {
		return nil
	}

	if err := s.releaseBulk(resultIDs...); err != nil {
		return s.setErr(err)
	}
	return s.DeleteUnreachableLinks()
}

func (s *ModifiableStore) releaseBulk(resultIDs ...string) error {
	stmt, err := s.tx.Prepare(deleteResultSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, resultID := range resultIDs {
		if _, err := stmt.Exec(resultID); err != nil {
			return err
		}
	}
	return nil
}

func (s *ModifiableStore) DeleteUnreachableLinks() error {
	if err := s.deleteUnreachableLinks(); err != nil {
		return s.setErr(err)
	}
	return nil
}

func (s *ModifiableStore) deleteUnreachableLinks() error {
	stmt, err := s.tx.Prepare(deleteUnreachableLinksSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for {
		res, err := stmt.Exec()
		if err != nil {
			return err
		}

		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return err
		}
	}
}

func (s *ModifiableStore) Clean() error {
	if err := s.clean(); err != nil {
		return s.setErr(err)
	}
	return nil
}

func (s *ModifiableStore) clean() error {
	_, err := s.tx.Exec(deleteUnusableLinksSQL)
	return err
}

func (s *ModifiableStore) DeleteUnreferenced(exists func(id string) bool) error {
	if err := s.deleteUnreferenced(exists); err != nil {
		return s.setErr(err)
	}
	return nil
}

func (s *ModifiableStore) deleteUnreferenced(exists func(id string) bool) error {
	res, err := s.tx.Query(walkDistinctResultsSQL)
	if err != nil {
		return err
	}
	defer res.Close()

	var resultIDs []string
	if err := s.walkDistinctResults(func(resultID string) {
		if !exists(resultID) {
			resultIDs = append(resultIDs, resultID)
		}
	}); err != nil {
		return err
	}
	return s.releaseBulk(resultIDs...)
}

func (s *ModifiableStore) walkDistinctResults(fn func(resultID string)) error {
	res, err := s.tx.Query(walkDistinctResultsSQL)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.Next() {
		var resultID string
		if err := res.Scan(&resultID); err != nil {
			return err
		}
		fn(resultID)
	}
	return nil
}

func (s *ModifiableStore) Commit() error {
	if s.err != nil {
		return s.err
	}
	return s.tx.Commit()
}

func (s *ModifiableStore) setErr(err error) error {
	if s.err == nil {
		s.tx.Rollback()
		s.err = err
	}
	return s.err
}

func isRootRef(id string) bool {
	return strings.IndexByte(id, ':') >= 0
}
