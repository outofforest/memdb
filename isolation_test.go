// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

//nolint:testifylint
package memdb_test

import (
	"testing"
	"unsafe"

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
		oldV, err := txn.Insert(0, unsafe.Pointer(obj1a))
		require.NoError(t, err)
		require.Nil(t, oldV)

		oldV, err = txn.Insert(0, unsafe.Pointer(obj3))
		require.NoError(t, err)
		require.Nil(t, oldV)
		txn.Commit()
		return db
	}

	t.Run("transaction dirty read", func(t *testing.T) {
		db := setup(t)

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		oldV, err := txn1.Insert(0, unsafe.Pointer(obj1b))
		require.NoError(t, err)
		require.NotNil(t, oldV)
		require.Equal(t, obj1a, (*TestObject)(*oldV))

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		oldV, err = txn1.Insert(0, unsafe.Pointer(obj2))
		require.NoError(t, err)
		require.Nil(t, oldV)

		txn2 := db.Txn(false)
		out, err := txn2.First(0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.Equal(t, "yep", (*TestObject)(*out).Baz)

		out, err = txn2.First(0, memdb.IDIndexID, id2)
		require.NoError(t, err)
		require.Nil(t, out)
	})

	t.Run("transaction non-repeatable read", func(t *testing.T) {
		db := setup(t)

		// Update an object
		obj1b := testObj()
		obj1b.ID = id1
		txn1 := db.Txn(true)
		obj1b.Baz = "nope"
		oldV, err := txn1.Insert(0, unsafe.Pointer(obj1b))
		require.NoError(t, err)
		require.NotNil(t, oldV)
		require.Equal(t, obj1a, (*TestObject)(*oldV))

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		oldV, err = txn1.Insert(0, unsafe.Pointer(obj2))
		require.NoError(t, err)
		require.NotNil(t, oldV)
		require.Equal(t, obj3, (*TestObject)(*oldV))

		txn2 := db.Txn(false)

		// Commit
		txn1.Commit()

		out, err := txn2.First(0, memdb.IDIndexID, id1)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.Equal(t, "yep", (*TestObject)(*out).Baz)

		out, err = txn2.First(0, memdb.IDIndexID, id2)
		require.NoError(t, err)
		require.Nil(t, out)
	})
}
