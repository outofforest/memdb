// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

//nolint:testifylint
package memdb_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
)

func TestTxn_Insert_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := testObj()
	oldV, err := txn.Insert(0, unsafe.Pointer(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	raw, err := txn.First(0, memdb.IDIndexID, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj, (*TestObject)(*raw))
}

func TestTxn_InsertUpdate_First(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  memdb.ID{1},
		Foo: "abc",
	}
	oldV, err := txn.Insert(0, unsafe.Pointer(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	raw, err := txn.First(0, memdb.IDIndexID, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj, (*TestObject)(*raw))

	// Update the object
	obj2 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	oldV, err = txn.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.NotNil(t, oldV)
	require.Equal(t, obj, (*TestObject)(*oldV))

	raw, err = txn.First(0, memdb.IDIndexID, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj2, (*TestObject)(*raw))
}

func TestTxn_InsertUpdate_First_NonUnique(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  memdb.ID{1},
		Foo: "abc",
	}
	oldV, err := txn.Insert(0, unsafe.Pointer(obj))
	require.NoError(t, err)
	require.Nil(t, oldV)

	raw, err := txn.First(0, indexFoo.ID(), obj.Foo)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj, (*TestObject)(*raw))

	// Update the object
	obj2 := &TestObject{
		ID:  memdb.ID{1},
		Foo: "xyz",
	}
	oldV, err = txn.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.NotNil(t, oldV)
	require.Equal(t, obj, (*TestObject)(*oldV))

	raw, err = txn.First(0, indexFoo.ID(), obj2.Foo)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj2, (*TestObject)(*raw))

	// Lookup of the old value should fail
	raw, err = txn.First(0, indexFoo.ID(), obj.Foo)
	require.NoError(t, err)
	require.Nil(t, raw)
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

	oldV, err := txn.Insert(0, unsafe.Pointer(obj))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, unsafe.Pointer(obj3))
	require.NoError(t, err)
	require.Nil(t, oldV)

	// The first object has a unique secondary value
	raw, err := txn.First(0, indexFoo.ID(), obj.Foo)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj, (*TestObject)(*raw))

	// Second and third object share secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First(0, indexFoo.ID(), obj2.Foo)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj2, (*TestObject)(*raw))
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

	oldV, err := txn.Insert(0, unsafe.Pointer(obj1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err := txn.First(0, indexFoo.ID(), obj2.Foo)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj1, (*TestObject)(*raw))

	// Commit and start a new transaction
	txn.Commit()
	txn = db.Txn(true)

	// Delete obj1
	oldV, err = txn.Delete(0, unsafe.Pointer(obj1))
	require.NoError(t, err)
	require.NotNil(t, oldV)
	require.Equal(t, obj1, (*TestObject)(*oldV))

	// Delete obj1 again and expect ErrNotFound
	oldV, err = txn.Delete(0, unsafe.Pointer(obj1))
	require.ErrorIs(t, err, memdb.ErrNotFound)
	require.Nil(t, oldV)

	// Lookup of the primary obj1 should fail
	raw, err = txn.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Nil(t, raw)

	// Commit and start a new read transaction
	txn.Commit()
	txn = db.Txn(false)

	// Lookup of the primary obj1 should fail
	raw, err = txn.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Nil(t, raw)

	// Check the shared secondary value,
	// but the primary ID of obj2 should be first
	raw, err = txn.First(0, indexFoo.ID(), obj2.Foo)
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.Equal(t, obj2, (*TestObject)(*raw))
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

	oldV, err := txn.Insert(0, unsafe.Pointer(obj1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	checkResult := func(txn *memdb.Txn) {
		// Attempt a row scan on the ID
		result, err := txn.Iterator(0, memdb.IDIndexID)
		require.NoError(t, err)
		require.Equal(t, obj1, (*TestObject)(*result.Next()))
		require.Equal(t, obj2, (*TestObject)(*result.Next()))
		require.Nil(t, result.Next())

		// Attempt a row scan on the ID with specific ID
		result, err = txn.Iterator(0, memdb.IDIndexID, obj1.ID)
		require.NoError(t, err)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		require.Equal(t, obj1, (*TestObject)(*result.Next()))
		require.Nil(t, result.Next())

		// Attempt a row scan secondary index
		result, err = txn.Iterator(0, indexFoo.ID(), obj1.Foo)
		require.NoError(t, err)
		require.Equal(t, obj1, (*TestObject)(*result.Next()))
		require.Equal(t, obj2, (*TestObject)(*result.Next()))
		require.Nil(t, result.Next())
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
	c := memdb.Config{
		Entities: []reflect.Type{
			reflect.TypeFor[TestObject](),
		},
		Indices: []memdb.Index{indexFoo},
	}

	db, err := memdb.NewMemDB(c)
	require.NoError(t, err)

	key := "aaaa"
	txn := db.Txn(true)
	oldV, err := txn.Insert(0, unsafe.Pointer(&TestObject{ID: memdb.ID{1}, Foo: key}))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, unsafe.Pointer(&TestObject{ID: memdb.ID{123}, Foo: key}))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(0, unsafe.Pointer(&TestObject{ID: memdb.ID{2}, Foo: key}))
	require.NoError(t, err)
	require.Nil(t, oldV)

	txn.Commit()

	txn = db.Txn(true)
	// Delete something
	oldV, err = txn.Delete(0, unsafe.Pointer(&TestObject{ID: memdb.ID{123}, Foo: key}))
	require.NoError(t, err)
	require.NotNil(t, oldV)
	require.Equal(t, &TestObject{ID: memdb.ID{123}, Foo: key}, (*TestObject)(*oldV))

	iter, err := txn.Iterator(0, indexFoo.ID(), key)
	require.NoError(t, err)

	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		_, err := txn.Delete(0, *obj)
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
				_, err := txn.Insert(0, unsafe.Pointer(&row))
				if err != nil {
					t.Fatalf("err inserting: %s", err)
				}
			}
			txn.Commit()

			txn = db.Txn(false)

			iterator, err := txn.Iterator(0, 0, memdb.From, tc.Search)
			if err != nil {
				t.Fatalf("err lower bound: %s", err)
			}

			// Now range scan and built a result set
			result := []TestObject{}
			for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
				result = append(result, *(*TestObject)(*obj))
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
		_, err := txn.Insert(0, unsafe.Pointer(&row))
		require.NoError(t, err)
	}
	txn.Commit()

	txn = db.Txn(false)

	iterator, err := txn.Iterator(0, 0, memdb.From, rows[5].ID, memdb.Back, 3)
	require.NoError(t, err)

	// Now range scan and built a result set
	result := []TestObject{}
	for obj := iterator.Next(); obj != nil; obj = iterator.Next() {
		result = append(result, *(*TestObject)(*obj))
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
