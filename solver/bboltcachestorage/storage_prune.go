package bboltcachestorage

import (
	"bytes"
	"context"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) Fix(ctx context.Context) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		validResults, err := s.pruneEmptyResults(tx)
		if err != nil {
			return err
		}

		if err := s.pruneInvalidLinks(tx, validResults); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) pruneEmptyResults(tx *bolt.Tx) (map[string]struct{}, error) {
	results := tx.Bucket([]byte(resultBucket))
	if results == nil {
		return nil, nil
	}

	isValid := make(map[string]struct{})

	var toDelete []string
	if err := results.ForEachBucket(func(id []byte) error {
		b := results.Bucket(id)
		if b == nil {
			return nil
		}

		if isEmptyBucket(b) {
			toDelete = append(toDelete, string(id))
			return nil
		}

		isValid[string(id)] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}

	for _, id := range toDelete {
		results.DeleteBucket([]byte(id))
	}
	return isValid, nil
}

func (s *Store) pruneInvalidLinks(tx *bolt.Tx, valid map[string]struct{}) error {
	links := tx.Bucket([]byte(linksBucket))
	if links == nil {
		return nil
	}

	var (
		toDelete        []string
		toDeleteEntries []string
	)
	if err := links.ForEachBucket(func(id []byte) error {
		// Name of the bucket matches the source result id.
		if _, ok := valid[string(id)]; !ok {
			// This bucket references a result that doesn't exist.
			toDelete = append(toDelete, string(id))
			return nil
		}

		toDeleteEntries = toDeleteEntries[:0]

		// Delete specific entries if they reference a result that
		// doesn't exist.
		keepBucket := false
		b := links.Bucket(id)
		if err := b.ForEach(func(k, _ []byte) error {
			index := bytes.LastIndexByte(k, '@')
			if index < 0 {
				return nil
			}

			target := k[index+1:]
			if _, ok := valid[string(target)]; !ok {
				toDeleteEntries = append(toDeleteEntries)
			} else {
				keepBucket = true
			}
			return nil
		}); err != nil {
			return err
		}

		if keepBucket {
			for _, key := range toDeleteEntries {
				_ = b.Delete([]byte(key))
			}
		} else {
			toDelete = append(toDelete, string(id))
		}
		return nil
	}); err != nil {
		return err
	}

	for _, id := range toDelete {
		links.DeleteBucket([]byte(id))
	}
	return nil
}
