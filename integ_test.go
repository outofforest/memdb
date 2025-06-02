// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"reflect"
	"testing"

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

	oldV, err := txn1.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	oldV, err = txn1.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	oldV, err = txn1.Insert(0, toReflectValue(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

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
func TestTxn_Abort(t *testing.T) {
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

	oldV, err := txn1.Insert(0, toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	oldV, err = txn1.Insert(0, toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	oldV, err = txn1.Insert(0, toReflectValue(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	require.Nil(t, oldV)

	// Abort the txn
	txn1.Abort()
	txn1.Commit()

	// Create a new transaction
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err := txn2.First(0, memdb.IDIndexID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}
}

func TestComplexDB(t *testing.T) {
	db := testComplexDB(t)
	testPopulateData(t, db)
	txn := db.Txn(false) // read only

	// Iterator using a full name
	raw, err := txn.First(peopleTableID, personNameIndex.ID(), "Armon", "Dadgar")
	require.NoError(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	raw, err = txn.First(peopleTableID, personAgeIndex.ID(), uint8(27))
	require.NoError(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	raw, err = txn.First(peopleTableID, personNegativeAgeIndex.ID(), int8(-26))
	require.NoError(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	person := fromReflectValue[TestPerson](raw)
	if person.First != "Armon" {
		t.Fatalf("wrong person!")
	}

	// Where in the world is mitchell hashimoto?
	raw, err = txn.First(peopleTableID, personNameIndex.ID(), "Mitchell")
	require.NoError(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	person = fromReflectValue[TestPerson](raw)
	if person.First != "Mitchell" {
		t.Fatalf("wrong person!")
	}
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
	oldV, err := txn.Insert(peopleTableID, toReflectValue(person1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(peopleTableID, toReflectValue(person2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(placesTableID, toReflectValue(place1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(placesTableID, toReflectValue(place2))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(placesTableID, toReflectValue(place3))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(visitsTableID, toReflectValue(visit1))
	require.NoError(t, err)
	require.Nil(t, oldV)

	oldV, err = txn.Insert(visitsTableID, toReflectValue(visit2))
	require.NoError(t, err)
	require.Nil(t, oldV)

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

func testComplexSchema() [][]memdb.Index {
	return [][]memdb.Index{
		{
			personNameIndex,
			personAgeIndex,
			personNegativeAgeIndex,
		},
		{
			placeNameIndex,
		},
		{},
	}
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

func toReflectValue(obj any) *reflect.Value {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	v2 := reflect.New(v.Type())
	v2.Elem().Set(v)
	return &v2
}

func fromReflectValue[T any](v *reflect.Value) T {
	return v.Elem().Interface().(T)
}
