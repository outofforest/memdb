// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
)

// Test that multiple concurrent transactions are isolated from each other.
func TestTxn_Isolation(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)

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

	oldV, err := txn1.Insert(0, unsafe.Pointer(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Zero(t, oldV)

	oldV, err = txn1.Insert(0, unsafe.Pointer(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Zero(t, oldV)

	oldV, err = txn1.Insert(0, unsafe.Pointer(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Zero(t, oldV)

	// Results should show up in this transaction
	raw, err := txn1.First(0, memdb.IDIndexID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Create a new transaction, current one is NOT committed
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err = txn2.First(0, memdb.IDIndexID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Commit txn1, txn2 should still be isolated
	txn1.Commit()

	// Nothing should show up in this transaction
	raw, err = txn2.First(0, memdb.IDIndexID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Create a new txn
	txn3 := db.Txn(false)

	// Results should show up in this transaction
	raw, err = txn3.First(0, memdb.IDIndexID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("bad: %#v", raw)
	}
}

// Test that an abort clears progress.
func TestTxn_DontCommit(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)

	obj1 := &TestObject{
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

	oldV, err := txn1.Insert(0, unsafe.Pointer(obj1))
	require.NoError(t, err)
	require.Zero(t, oldV)

	oldV, err = txn1.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.Zero(t, oldV)

	oldV, err = txn1.Insert(0, unsafe.Pointer(obj3))
	require.NoError(t, err)
	require.Zero(t, oldV)

	// Leve the tx uncommitted.

	// Create a new transaction
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err := txn2.First(0, memdb.IDIndexID)
	require.NoError(t, err)
	require.Zero(t, raw)
}

func TestTxn_SubTx(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)

	obj1 := &TestObject{
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

	// Init txn1

	oldV, err := txn1.Insert(0, unsafe.Pointer(obj1))
	require.NoError(t, err)
	require.Zero(t, oldV)

	oldV, err = txn1.Insert(0, unsafe.Pointer(obj2))
	require.NoError(t, err)
	require.Zero(t, oldV)

	// Create subtransaction transaction
	txn2 := txn1.Txn(true)

	// Also create new top transaction.
	txn3 := db.Txn(false)

	// Remove object from txn2
	oldV, err = txn2.Delete(0, unsafe.Pointer(obj1))
	require.NoError(t, err)
	require.NotZero(t, oldV)

	// Add object to txn2
	oldV, err = txn2.Insert(0, unsafe.Pointer(obj3))
	require.NoError(t, err)
	require.Zero(t, oldV)

	// Verify that changes are not visible in txn1.
	v, err := txn1.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.NotZero(t, v)

	v, err = txn1.First(0, memdb.IDIndexID, obj3.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	// Verify that changes are visible in txn2.
	v, err = txn2.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn2.First(0, memdb.IDIndexID, obj3.ID)
	require.NoError(t, err)
	require.NotZero(t, v)

	// Verify that state from txn1 is visible in txn2.
	v, err = txn2.First(0, memdb.IDIndexID, obj2.ID)
	require.NoError(t, err)
	require.NotZero(t, v)

	// Commit txn2
	txn2.Commit()

	// Also create new top transaction.
	txn4 := db.Txn(false)

	// Verify that changes are visible in txn1.
	v, err = txn1.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn1.First(0, memdb.IDIndexID, obj3.ID)
	require.NoError(t, err)
	require.NotZero(t, v)

	// Verify that changes are not visible in the other top transaction.
	v, err = txn3.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn3.First(0, memdb.IDIndexID, obj2.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn3.First(0, memdb.IDIndexID, obj3.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn4.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn4.First(0, memdb.IDIndexID, obj2.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn4.First(0, memdb.IDIndexID, obj3.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	// Commit top transaction.
	txn1.Commit()

	// Create new top transaction.
	txn5 := db.Txn(false)

	// Verify that entities are visible.
	v, err = txn5.First(0, memdb.IDIndexID, obj1.ID)
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = txn5.First(0, memdb.IDIndexID, obj2.ID)
	require.NoError(t, err)
	require.NotZero(t, v)

	v, err = txn5.First(0, memdb.IDIndexID, obj3.ID)
	require.NoError(t, err)
	require.NotZero(t, v)
}

func TestTxn_ReadOnlySubTxFailsOnCommit(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)
	txn2 := txn1.Txn(false)

	require.Panics(t, func() {
		txn2.Commit()
	})
}

func TestTxn_SubTxFailsOnConcurrentCommit(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(true)
	txn2 := txn1.Txn(true)
	txn3 := txn1.Txn(true)

	txn2.Commit()
	require.Panics(t, func() {
		txn3.Commit()
	})
}

func TestTxn_SubTxMayCommitToReadOnlyTx(t *testing.T) {
	db := testDB(t)
	txn1 := db.Txn(false)
	txn2 := txn1.Txn(true)

	txn2.Commit()
	require.Panics(t, func() {
		txn1.Commit()
	})
}

func TestComplexDB(t *testing.T) {
	db := testComplexDB(t)
	testPopulateData(t, db)
	txn := db.Txn(false) // read only

	// Iterator using a full name
	person, err := txn.First(peopleTableID, personNameIndex.ID(), "Armon", "Dadgar")
	require.NoError(t, err)
	require.NotZero(t, person)

	person, err = txn.First(peopleTableID, personAgeIndex.ID(), uint8(27))
	require.NoError(t, err)
	require.NotZero(t, person)

	person, err = txn.First(peopleTableID, personNegativeAgeIndex.ID(), int8(-26))
	require.NoError(t, err)
	require.NotZero(t, person)
	require.Equal(t, "Armon", (*TestPerson)(person).First)

	// Where in the world is mitchell hashimoto?
	person, err = txn.First(peopleTableID, personNameIndex.ID(), "Mitchell")
	require.NoError(t, err)
	require.NotZero(t, person)
	require.Equal(t, "Mitchell", (*TestPerson)(person).First)
}

type TestObject struct {
	ID     memdb.ID
	Foo    string
	Baz    string
	Empty  string
	Int8   int8
	Int16  int16
	Int32  int32
	Int64  int64
	Uint   uint
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64
	Bool   bool
}

func String(s string) *string {
	return &s
}

func testObj() *TestObject {
	obj := &TestObject{
		ID:     memdb.ID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Foo:    "Testing",
		Baz:    "yep",
		Int8:   int8(-1 << 7),
		Int16:  int16(-1 << 15),
		Int32:  int32(-1 << 31),
		Int64:  int64(-1 << 63),
		Uint:   uint(1),
		Uint8:  uint8(1<<8 - 1),
		Uint16: uint16(1<<16 - 1),
		Uint32: uint32(1<<32 - 1),
		Uint64: uint64(1<<64 - 1),
		Bool:   false,
	}
	return obj
}

func testPopulateData(t *testing.T, db *memdb.MemDB) {
	// Start write txn
	txn := db.Txn(true)

	// Create some data
	person1 := testPerson()

	person2 := testPerson()
	person2.First = "Mitchell"
	person2.Last = "Hashimoto"
	person2.Age = 27
	person2.NegativeAge = -27

	place1 := testPlace()
	place2 := testPlace()
	place2.Name = "Maui"
	place3 := testPlace()

	visit1 := testVisit(person1.ID, place1.ID)
	visit2 := testVisit(person2.ID, place2.ID)

	// Insert it all
	oldPerson, err := txn.Insert(peopleTableID, unsafe.Pointer(&person1))
	require.NoError(t, err)
	require.Zero(t, oldPerson)

	oldPerson, err = txn.Insert(peopleTableID, unsafe.Pointer(&person2))
	require.NoError(t, err)
	require.Zero(t, oldPerson)

	oldPlace, err := txn.Insert(placesTableID, unsafe.Pointer(&place1))
	require.NoError(t, err)
	require.Zero(t, oldPlace)

	oldPlace, err = txn.Insert(placesTableID, unsafe.Pointer(&place2))
	require.NoError(t, err)
	require.Zero(t, oldPlace)

	oldPlace, err = txn.Insert(placesTableID, unsafe.Pointer(&place3))
	require.NoError(t, err)
	require.Zero(t, oldPlace)

	oldVisit, err := txn.Insert(visitsTableID, unsafe.Pointer(&visit1))
	require.NoError(t, err)
	require.Zero(t, oldVisit)

	oldVisit, err = txn.Insert(visitsTableID, unsafe.Pointer(&visit2))
	require.NoError(t, err)
	require.Zero(t, oldVisit)

	// Commit
	txn.Commit()
}

type TestPerson struct {
	ID          memdb.ID
	First       string
	Last        string
	Age         uint8
	NegativeAge int8
}

type TestPlace struct {
	ID   memdb.ID
	Name string
}

type TestVisit struct {
	ID     memdb.ID
	Person memdb.ID
	Place  memdb.ID
}

const (
	peopleTableID uint64 = iota
	placesTableID
	visitsTableID
)

var (
	person          TestPerson
	personNameIndex = indices.NewMultiIndex(
		indices.NewFieldIndex(&person, &person.First),
		indices.NewFieldIndex(&person, &person.Last),
	)
	personAgeIndex         = indices.NewFieldIndex(&person, &person.Age)
	personNegativeAgeIndex = indices.NewFieldIndex(&person, &person.NegativeAge)

	place          TestPlace
	placeNameIndex = indices.NewFieldIndex(&place, &place.Name)
)

func testComplexSchema() memdb.Config {
	c := memdb.Config{
		Entities: []reflect.Type{
			reflect.TypeFor[TestPerson](),
			reflect.TypeFor[TestPlace](),
			reflect.TypeFor[TestVisit](),
		},
		Indices: []memdb.Index{
			personNameIndex,
			personAgeIndex,
			personNegativeAgeIndex,
			placeNameIndex,
		},
	}

	return c
}

func testComplexDB(t *testing.T) *memdb.MemDB {
	db, err := memdb.NewMemDB(testComplexSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}

func testPerson() TestPerson {
	return TestPerson{
		ID:          memdb.NewID[memdb.ID](),
		First:       "Armon",
		Last:        "Dadgar",
		Age:         26,
		NegativeAge: -26,
	}
}

func testPlace() TestPlace {
	return TestPlace{
		ID:   memdb.NewID[memdb.ID](),
		Name: "HashiCorp",
	}
}

func testVisit(personID, placeID memdb.ID) TestVisit {
	return TestVisit{
		ID:     memdb.NewID[memdb.ID](),
		Person: personID,
		Place:  placeID,
	}
}
