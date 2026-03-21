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

func TestMemDB_Snapshot(t *testing.T) {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add an object
	obj := testObj()
	txn := db.Txn(true)
	oldV, err := memdb.Insert(txn, 0, obj)
	require.NoError(t, err)
	require.Nil(t, oldV)
	txn.Commit()

	// Clone the db
	db2 := db.Snapshot()

	// Remove the object
	txn = db.Txn(true)
	oldV, err = memdb.Delete(txn, 0, obj)
	require.NoError(t, err)
	require.Equal(t, obj, oldV)
	txn.Commit()

	// Object should exist in second snapshot but not first
	txn = db.Txn(false)
	out, err := memdb.First[TestObject](txn, 0, memdb.IDIndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Fatalf("should not exist %#v", out)
	}

	txn = db2.Txn(true)
	out, err = memdb.First[TestObject](txn, 0, memdb.IDIndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil {
		t.Fatalf("should exist")
	}
}
