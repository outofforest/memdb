// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/id"
)

func TestTxn_Read_AbortCommit(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(false) // Readonly

	txn.Abort()
	txn.Abort()
	txn.Commit()
	txn.Commit()
}

func TestTxn_Write_AbortCommit(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true) // Write

	txn.Abort()
	txn.Abort()
	txn.Commit()
	txn.Commit()

	txn = db.Txn(true) // Write

	txn.Commit()
	txn.Commit()
	txn.Abort()
	txn.Abort()
}

func TestTxn_Insert_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := testObj()
	err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First(0, id.IndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if fromReflectValue[TestObject](raw) != *obj {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), obj)
	}
}

func TestTxn_InsertUpdate_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  memdb.ID{1},
		Foo: "abc",
	}
	err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First(0, id.IndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if fromReflectValue[TestObject](raw) != *obj {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First(0, id.IndexID, obj.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if fromReflectValue[TestObject](raw) != *obj2 {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj)
	}
}

func TestTxn_InsertUpdate_First_NonUnique(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  memdb.ID{1},
		Foo: "abc",
	}
	err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err := txn.First(0, indexFoo.ID(), obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if fromReflectValue[TestObject](raw) != *obj {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj)
	}

	// Update the object
	obj2 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	raw, err = txn.First(0, indexFoo.ID(), obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if fromReflectValue[TestObject](raw) != *obj2 {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj2)
	}

	// Lookup of the old value should fail
	raw, err = txn.First(0, indexFoo.ID(), obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestTxn_First_NonUnique_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  memdb.ID{1},
		Foo: "abc",
	}
	obj2 := &TestObject{
		ID:  memdb.ID{2},
		Foo: "xyz",
	}
	obj3 := &TestObject{
		ID:  memdb.ID{3},
		Foo: "xyz",
	}

	err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The first object has a unique secondary value
	raw, err := txn.First(0, indexFoo.ID(), obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fromReflectValue[TestObject](raw) != *obj {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First(0, indexFoo.ID(), obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fromReflectValue[TestObject](raw) != *obj2 {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), obj2)
	}
}

func TestTxn_Last_NonUnique_Multiple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	obj2 := &TestObject{
		ID:  memdb.ID{2},
		Foo: "abc",
	}
	obj3 := &TestObject{
		ID:  memdb.ID{3},
		Foo: "abc",
	}

	err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The last object has a unique secondary value
	raw, err := txn.Last(0, indexFoo.ID(), obj.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fromReflectValue[TestObject](raw) != *obj {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj)
	}

	// Second and third object share secondary value,
	// but the primary ID of obj3 should be last
	raw, err = txn.Last(0, indexFoo.ID(), obj3.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fromReflectValue[TestObject](raw) != *obj3 {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj3)
	}
}

func TestTxn_InsertDelete_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	obj2 := &TestObject{
		ID:  memdb.ID{2},
		Foo: "xyz",
	}

	err := txn.Insert(0, toReflectValue(obj1))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err := txn.First(0, indexFoo.ID(), obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fromReflectValue[TestObject](raw) != *obj1 {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
	}

	// Commit and start a new transaction
	txn.Commit()
	txn = db.Txn(true)

	// Delete obj1
	err = txn.Delete(0, toReflectValue(obj1))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete obj1 again and expect ErrNotFound
	err = txn.Delete(0, toReflectValue(obj1))
	if !errors.Is(err, memdb.ErrNotFound) {
		t.Fatalf("expected err to be %v, got %v", memdb.ErrNotFound, err)
	}

	// Lookup of the primary obj1 should fail
	raw, err = txn.First(0, id.IndexID, obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Lookup of the primary obj1 should fail
	raw, err = txn.First(0, id.IndexID, obj1.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v %#v", raw, obj1)
	}

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First(0, indexFoo.ID(), obj2.Foo)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fromReflectValue[TestObject](raw) != *obj2 {
		t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj2)
	}
}

func TestTxn_InsertGet_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	obj2 := &TestObject{
		ID:  memdb.ID{2},
		Foo: "xyz",
	}

	err := txn.Insert(0, toReflectValue(obj1))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *memdb.Txn) {
		// Attempt a row scan on the ID
		result, err := txn.Get(0, id.IndexID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj1 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj2 {
			t.Fatalf("bad: %#v %#v", raw, obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID
		result, err = txn.Get(0, id.IndexID, obj1.ID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj1 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.Get(0, indexFoo.ID(), obj1.Foo)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj1 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj2 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj2)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

func TestTxn_InsertGetReverse_Simple(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj1 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	obj2 := &TestObject{
		ID:  memdb.ID{2},
		Foo: "xyz",
	}

	err := txn.Insert(0, toReflectValue(obj1))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	checkResult := func(txn *memdb.Txn) {
		// Attempt a row scan on the ID
		result, err := txn.GetReverse(0, id.IndexID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj2 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj2)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj1 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan on the ID with specific ID
		result, err = txn.GetReverse(0, id.IndexID, obj1.ID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj1 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}

		// Attempt a row scan secondary index
		result, err = txn.GetReverse(0, indexFoo.ID(), obj2.Foo)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj2 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj2)
		}

		if raw := result.Next(); fromReflectValue[TestObject](raw) != *obj1 {
			t.Fatalf("bad: %#v %#v", fromReflectValue[TestObject](raw), *obj1)
		}

		if raw := result.Next(); raw != nil {
			t.Fatalf("bad: %#v %#v", raw, nil)
		}
	}

	// Check the results within the txn
	checkResult(txn)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Check the results in a new txn
	checkResult(txn)
}

func TestTxn_GetIterAndDelete(t *testing.T) {
	db, err := memdb.NewMemDB([][]memdb.Index{{indexFoo}})
	assertNilError(t, err)

	key := "aaaa"
	txn := db.Txn(true)
	assertNilError(t, txn.Insert(0, toReflectValue(TestObject{ID: memdb.ID{1}, Foo: key})))
	assertNilError(t, txn.Insert(0, toReflectValue(TestObject{ID: memdb.ID{123}, Foo: key})))
	assertNilError(t, txn.Insert(0, toReflectValue(TestObject{ID: memdb.ID{2}, Foo: key})))
	txn.Commit()

	txn = db.Txn(true)
	// Delete something
	assertNilError(t, txn.Delete(0, toReflectValue(TestObject{ID: memdb.ID{123}, Foo: key})))

	iter, err := txn.Get(0, indexFoo.ID(), key)
	assertNilError(t, err)

	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		assertNilError(t, txn.Delete(0, obj))
	}

	txn.Commit()
}

func testDB(t *testing.T) *memdb.MemDB {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}

func assertNilError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
