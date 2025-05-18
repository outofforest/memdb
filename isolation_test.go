// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

//nolint:goconst
package memdb_test

import (
	"testing"

	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/id"
)

func TestMemDB_Isolation(t *testing.T) {
	id1 := memdb.ID{1}
	id2 := memdb.ID{2}
	id3 := memdb.ID{3}

	mustNoError := func(t *testing.T, err error) {
		if err != nil {
			t.Fatalf("unexpected test error: %v", err)
		}
	}

	setup := func(t *testing.T) *memdb.MemDB {
		t.Helper()

		db, err := memdb.NewMemDB(testValidSchema())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		// Add two objects (with a gap between their IDs)
		obj1a := testObj()
		obj1a.ID = id1
		txn := db.Txn(true)
		mustNoError(t, txn.Insert(0, toReflectValue(obj1a)))

		obj3 := testObj()
		obj3.ID = id3
		mustNoError(t, txn.Insert(0, toReflectValue(obj3)))
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
		mustNoError(t, txn1.Insert(0, toReflectValue(obj1b)))

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		mustNoError(t, txn1.Insert(0, toReflectValue(obj2)))

		txn2 := db2.Txn(false)
		out, err := txn2.First(0, id.IndexID, id1)
		mustNoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if fromReflectValue[TestObject](out).Baz == "nope" {
			t.Fatalf("read from snapshot should not observe uncommitted update (dirty read)")
		}

		out, err = txn2.First(0, id.IndexID, id2)
		mustNoError(t, err)
		if out != nil {
			t.Fatalf("read from snapshot should not observe uncommitted insert (dirty read)")
		}

		// New snapshot should not observe uncommitted writes
		db3 := db.Snapshot()
		txn3 := db3.Txn(false)
		out, err = txn3.First(0, id.IndexID, id1)
		mustNoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if fromReflectValue[TestObject](out).Baz == "nope" {
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
		mustNoError(t, txn1.Insert(0, toReflectValue(obj1b)))

		// Insert an object
		obj2 := testObj()
		obj2.ID = id2
		mustNoError(t, txn1.Insert(0, toReflectValue(obj2)))

		txn2 := db.Txn(false)
		out, err := txn2.First(0, id.IndexID, id1)
		mustNoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if fromReflectValue[TestObject](out).Baz == "nope" {
			t.Fatalf("read from transaction should not observe uncommitted update (dirty read)")
		}

		out, err = txn2.First(0, id.IndexID, id2)
		mustNoError(t, err)
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
		mustNoError(t, txn1.Insert(0, toReflectValue(obj1b)))

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		mustNoError(t, txn1.Insert(0, toReflectValue(obj2)))

		// Commit
		txn1.Commit()

		txn2 := db2.Txn(false)
		out, err := txn2.First(0, id.IndexID, id1)
		mustNoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if fromReflectValue[TestObject](out).Baz == "nope" {
			t.Fatalf("read from snapshot should not observe committed write from another transaction (non-repeatable read)")
		}

		out, err = txn2.First(0, id.IndexID, id2)
		mustNoError(t, err)
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
		mustNoError(t, txn1.Insert(0, toReflectValue(obj1b)))

		// Insert an object
		obj2 := testObj()
		obj2.ID = id3
		mustNoError(t, txn1.Insert(0, toReflectValue(obj2)))

		txn2 := db.Txn(false)

		// Commit
		txn1.Commit()

		out, err := txn2.First(0, id.IndexID, id1)
		mustNoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if fromReflectValue[TestObject](out).Baz == "nope" {
			t.Fatalf("read from transaction should not observe committed write from another transaction (non-repeatable read)")
		}

		out, err = txn2.First(0, id.IndexID, id2)
		mustNoError(t, err)
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
		mustNoError(t, txn2.Insert(0, toReflectValue(obj1)))
		txn2.Commit()

		txn1 := db.Txn(false)
		out, err := txn1.First(0, id.IndexID, id1)
		mustNoError(t, err)
		if out == nil {
			t.Fatalf("should exist")
		}
		if fromReflectValue[TestObject](out).Baz == "also" {
			t.Fatalf("commit from snapshot should never be observed")
		}
	})
}
