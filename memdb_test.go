// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
)

func TestMemDB_SingleWriter_MultiReader(t *testing.T) {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	tx1 := db.Txn(true)
	tx2 := db.Txn(false) // Should not block!
	tx3 := db.Txn(false) // Should not block!
	tx4 := db.Txn(false) // Should not block!

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		db.Txn(true)
	}()

	select {
	case <-doneCh:
		t.Fatalf("should not allow another writer")
	case <-time.After(10 * time.Millisecond):
	}

	tx1.Abort()
	tx2.Abort()
	tx3.Abort()
	tx4.Abort()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("should allow another writer")
	}
}

func TestMemDB_Snapshot(t *testing.T) {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add an object
	obj := testObj()
	txn := db.Txn(true)
	oldV, err := txn.Insert(0, toReflectValue(obj))
	require.NoError(t, err)
	require.Nil(t, oldV)
	txn.Commit()

	// Clone the db
	db2 := db.Snapshot()

	// Remove the object
	txn = db.Txn(true)
	oldV, err = txn.Delete(0, toReflectValue(obj))
	require.NoError(t, err)
	require.Equal(t, *obj, fromReflectValue[TestObject](oldV))
	txn.Commit()

	// Object should exist in second snapshot but not first
	txn = db.Txn(false)
	out, err := txn.First(0, memdb.IDIndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != nil {
		t.Fatalf("should not exist %#v", out)
	}

	txn = db2.Txn(true)
	out, err = txn.First(0, memdb.IDIndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil {
		t.Fatalf("should exist")
	}
}
