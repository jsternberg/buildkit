package main

import (
	"context"
	"os"

	"github.com/moby/buildkit/solver/bboltcachestorage"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
)

var FixCommand = &cobra.Command{
	Use:  "fix",
	Args: cobra.ExactArgs(1),
	RunE: fixE,
}

func fixE(cmd *cobra.Command, args []string) error {
	dbPath := args[0]

	if err := fixDB(dbPath); err != nil {
		return err
	}
	return compact(dbPath)
}

func fixDB(dbPath string) error {
	db, err := bboltcachestorage.NewStore(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Fix(context.TODO())
}

func compact(dbPath string) error {
	fi, err := os.Stat(dbPath)
	if err != nil {
		return err
	}

	mode := fi.Mode()

	src, err := bbolt.Open(dbPath, mode, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer src.Close()

	tmpPath := dbPath + ".tmp"
	dst, err := bbolt.Open(tmpPath, mode, nil)
	if err != nil {
		return err
	}
	defer dst.Close()

	defer os.Remove(tmpPath)

	if err := bbolt.Compact(dst, src, 0); err != nil {
		return err
	}
	return os.Rename(tmpPath, dbPath)
}
