// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

//nolint:goconst
package memdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
)

func TestMemDB_Isolation(t *testing.T) {
	id1 := memdb.ID{1}
	id2 := memdb.ID{2}
	id3 := memdb.ID{3}

	obj1a := testObj()
	obj1a.ID = id1

	obj3 := testObj()
	obj3.ID = id3

	setup := func(t *testing.T) *memdb.MemDB {
		t.Helper()

		db, err := memdb.NewMemDB(testValidSchema())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		// Add two objects (with a gap between their IDs)
		txn := db.Txn(true)
		oldV, err := memdb.Insert(txn, 0, obj1a)
		require.NoError(t, err)
		require.Nil(t, oldV)

		oldV, err = memdb.Insert(txn, 0, obj3)
		require.NoError(t, err)
		require.Nil(t, oldV)
		txn.Commit()
		return db
	}

	t.Run("snapshot dirty read", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		oldV, err := memdb.Insert(txn1, 0, obj1b)
		require.NoError(t, err)
		require.Equal(t, obj1a, oldV)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		oldV, err = memdb.Insert(txn1, 0, obj2)
		require.NoError(t, err)
		require.Nil(t, oldV)

		txn2 := db2.Txn(false)
		out, err := memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.Baz == "nope" {
			t.Fatalf("read from snapshot should not observe uncommitted update (dirty read)")
		}

		out, err = memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id2)
		require.NoError(t, err)
		if out != nil {
			t.Fatalf("read from snapshot should not observe uncommitted insert (dirty read)")
		}

		// New snapshot should not observe uncommitted writes
		db3 := db.Snapshot()
		txn3 := db3.Txn(false)
		out, err = memdb.First[TestObject](txn3, 0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.Baz == "nope" {
			t.Fatalf("read from new snapshot should not observe uncommitted writes")
		}
	})

	t.Run("transaction dirty read", func(t *testing.T) {
		db := setup(t)

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		oldV, err := memdb.Insert(txn1, 0, obj1b)
		require.NoError(t, err)
		require.Equal(t, obj1a, oldV)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		oldV, err = memdb.Insert(txn1, 0, obj2)
		require.NoError(t, err)
		require.Nil(t, oldV)

		txn2 := db.Txn(false)
		out, err := memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.Baz == "nope" {
			t.Fatalf("read from transaction should not observe uncommitted update (dirty read)")
		}

		out, err = memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id2)
		require.NoError(t, err)
		if out != nil {
			t.Fatalf("read from transaction should not observe uncommitted insert (dirty read)")
		}
	})

	t.Run("snapshot non-repeatable read", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		oldV, err := memdb.Insert(txn1, 0, obj1b)
		require.NoError(t, err)
		require.Equal(t, obj1a, oldV)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		oldV, err = memdb.Insert[TestObject](txn1, 0, obj2)
		require.NoError(t, err)
		require.Equal(t, obj3, oldV)

		// Commit
		txn1.Commit()

		txn2 := db2.Txn(false)
		out, err := memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.Baz == "nope" {
			t.Fatalf("read from snapshot should not observe committed write from another transaction (non-repeatable read)")
		}

		out, err = memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id2)
		require.NoError(t, err)
		if out != nil {
			t.Fatalf("read from snapshot should not observe committed write from another transaction (non-repeatable read)")
		}
	})

	t.Run("transaction non-repeatable read", func(t *testing.T) {
		db := setup(t)

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		oldV, err := memdb.Insert(txn1, 0, obj1b)
		require.NoError(t, err)
		require.Equal(t, obj1a, oldV)

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		oldV, err = memdb.Insert(txn1, 0, obj2)
		require.NoError(t, err)
		require.Equal(t, obj3, oldV)

		txn2 := db.Txn(false)

		// Commit
		txn1.Commit()

		out, err := memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.Baz == "nope" {
			t.Fatalf("read from transaction should not observe committed write from another transaction (non-repeatable read)")
		}

		out, err = memdb.First[TestObject](txn2, 0, memdb.IDIndexID, id2)
		require.NoError(t, err)
		if out != nil {
			t.Fatalf("read from transaction should not observe committed write from another transaction (non-repeatable read)")
		}
	})

	t.Run("snapshot commits are unobservable", func(t *testing.T) {
		db := setup(t)
		db2 := db.Snapshot()

		txn2 := db2.Txn(true)
		obj1 := testObj()
		obj1.ID = id1
		obj1.Baz = "also"
		oldV, err := memdb.Insert(txn2, 0, obj1)
		require.NoError(t, err)
		require.Equal(t, obj1a, oldV)
		txn2.Commit()

		txn1 := db.Txn(false)
		out, err := memdb.First[TestObject](txn1, 0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if out.Baz == "also" {
			t.Fatalf("commit from snapshot should never be observed")
		}
	})
}
