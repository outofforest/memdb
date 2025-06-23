// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
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
	oldV, err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	raw, err := txn.First(0, memdb.IDIndexID, obj.ID)
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
	oldV, err := txn.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	raw, err := txn.First(0, memdb.IDIndexID, obj.ID)
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
	oldV, err = txn.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Equal(t, *obj, fromReflectValue[TestObject](oldV))

	raw, err = txn.First(0, memdb.IDIndexID, obj.ID)
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
	oldV, err := txn.Insert(0, toReflectValue(obj))
	require.NoError(t, err)
	require.Nil(t, oldV)

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
	oldV, err = txn.Insert(0, toReflectValue(obj2))
	require.NoError(t, err)
	require.Equal(t, *obj, fromReflectValue[TestObject](oldV))

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

	oldV, err := txn.Insert(0, toReflectValue(obj))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, toReflectValue(obj2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, toReflectValue(obj3))
	require.NoError(t, err)
	require.Nil(t, oldV)

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

	oldV, err := txn.Insert(0, toReflectValue(obj1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, toReflectValue(obj2))
	require.NoError(t, err)
	require.Nil(t, oldV)

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
	oldV, err = txn.Delete(0, toReflectValue(obj1))
	require.NoError(t, err)
	require.Equal(t, *obj1, fromReflectValue[TestObject](oldV))

	// Delete obj1 again and expect ErrNotFound
	oldV, err = txn.Delete(0, toReflectValue(obj1))
	require.ErrorIs(t, err, memdb.ErrNotFound)
	require.Nil(t, oldV)

	// Lookup of the primary obj1 should fail
	raw, err = txn.First(0, memdb.IDIndexID, obj1.ID)
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
	raw, err = txn.First(0, memdb.IDIndexID, obj1.ID)
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

	oldV, err := txn.Insert(0, toReflectValue(obj1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, toReflectValue(obj2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	checkResult := func(txn *memdb.Txn) {
		// Attempt a row scan on the ID
		result, err := txn.Iterator(0, memdb.IDIndexID)
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
		result, err = txn.Iterator(0, memdb.IDIndexID, obj1.ID)
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
		result, err = txn.Iterator(0, indexFoo.ID(), obj1.Foo)
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

func TestTxn_GetIterAndDelete(t *testing.T) {
	db, err := memdb.NewMemDB([][]memdb.Index{{indexFoo}})
	require.NoError(t, err)

	key := "aaaa"
	txn := db.Txn(true)
	oldV, err := txn.Insert(0, toReflectValue(TestObject{ID: memdb.ID{1}, Foo: key}))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, toReflectValue(TestObject{ID: memdb.ID{123}, Foo: key}))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, toReflectValue(TestObject{ID: memdb.ID{2}, Foo: key}))
	require.NoError(t, err)
	require.Nil(t, oldV)

	txn.Commit()

	txn = db.Txn(true)
	// Delete something
	oldV, err = txn.Delete(0, toReflectValue(TestObject{ID: memdb.ID{123}, Foo: key}))
	require.NoError(t, err)
	require.Equal(t, TestObject{ID: memdb.ID{123}, Foo: key}, fromReflectValue[TestObject](oldV))

	iter, err := txn.Iterator(0, indexFoo.ID(), key)
	require.NoError(t, err)

	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		_, err := txn.Delete(0, obj)
		require.NoError(t, err)
	}

	txn.Commit()
}

func TestTxn_LowerBound(t *testing.T) {
	basicRows := []TestObject{
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x01}, Foo: "1"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x02}, Foo: "2"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x04}, Foo: "3"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x05}, Foo: "4"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x01, 0x00}, Foo: "5"},
		{ID: memdb.ID{0x01, 0x00, 0x00, 0x01, 0x00}, Foo: "6"},
	}

	cases := []struct {
		Name   string
		Rows   []TestObject
		Search memdb.ID
		Want   []TestObject
	}{
		{
			Name:   "all",
			Rows:   basicRows,
			Search: memdb.ID{},
			Want:   basicRows,
		},
		{
			Name:   "subset existing bound",
			Rows:   basicRows,
			Search: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x05},
			Want: []TestObject{
				{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x05}, Foo: "4"},
				{ID: memdb.ID{0x00, 0x00, 0x00, 0x01, 0x00}, Foo: "5"},
				{ID: memdb.ID{0x01, 0x00, 0x00, 0x01, 0x00}, Foo: "6"},
			},
		},
		{
			Name:   "subset non-existent bound",
			Rows:   basicRows,
			Search: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x06},
			Want: []TestObject{
				{ID: memdb.ID{0x00, 0x00, 0x00, 0x01, 0x00}, Foo: "5"},
				{ID: memdb.ID{0x01, 0x00, 0x00, 0x01, 0x00}, Foo: "6"},
			},
		},
		{
			Name:   "empty subset",
			Rows:   basicRows,
			Search: memdb.ID{0x09, 0x09, 0x09, 0x09, 0x09},
			Want:   []TestObject{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			db := testDB(t)

			txn := db.Txn(true)
			for _, row := range tc.Rows {
				_, err := txn.Insert(0, toReflectValue(row))
				if err != nil {
					t.Fatalf("err inserting: %s", err)
				}
			}
			txn.Commit()

			txn = db.Txn(false)
			defer txn.Abort()
			iterator, err := txn.Iterator(0, 0, memdb.From, tc.Search)
			if err != nil {
				t.Fatalf("err lower bound: %s", err)
			}

			// Now range scan and built a result set
			result := []TestObject{}
			for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
				result = append(result, fromReflectValue[TestObject](obj))
			}

			if !reflect.DeepEqual(result, tc.Want) {
				t.Fatalf(" got: %#v\nwant: %#v", result, tc.Want)
			}
		})
	}
}

func TestTxn_Back(t *testing.T) {
	rows := []TestObject{
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x01}, Foo: "1"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x02}, Foo: "2"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x04}, Foo: "3"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x00, 0x05}, Foo: "4"},
		{ID: memdb.ID{0x00, 0x00, 0x00, 0x01, 0x00}, Foo: "5"},
		{ID: memdb.ID{0x01, 0x00, 0x00, 0x01, 0x00}, Foo: "6"},
	}

	db := testDB(t)

	txn := db.Txn(true)
	for _, row := range rows {
		_, err := txn.Insert(0, toReflectValue(row))
		require.NoError(t, err)
	}
	txn.Commit()

	txn = db.Txn(false)
	defer txn.Abort()
	iterator, err := txn.Iterator(0, 0, memdb.From, rows[5].ID)
	require.NoError(t, err)

	iterator.Back(3)

	// Now range scan and built a result set
	result := []TestObject{}
	for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
		result = append(result, fromReflectValue[TestObject](obj))
	}

	require.Equal(t, rows[2:], result)
}

func testDB(t *testing.T) *memdb.MemDB {
	db, err := memdb.NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}
