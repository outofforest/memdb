// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
)

func TestMemDB_PanicOnParallelCommit(t *testing.T) {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	tx1 := db.Txn(true)
	tx2 := db.Txn(true)

	tx1.Commit()
	require.Panics(t, func() {
		tx2.Commit()
	})
}

func TestMemDB_PanicOnReadOnlyCommit(t *testing.T) {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	tx := db.Txn(false)
	require.Panics(t, func() {
		tx.Commit()
	})
}

func TestMemDB_PanicOnDoubleCommit(t *testing.T) {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	tx := db.Txn(true)
	tx.Commit()
	require.Panics(t, func() {
		tx.Commit()
	})
}
