// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"reflect"
	"testing"

	"github.com/outofforest/go-memdb"
	"github.com/outofforest/go-memdb/indices"
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

	err := txn1.Insert("main", toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", toReflectValue(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Results should show up in this transaction
	raw, err := txn1.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw == nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Create a new transaction, current one is NOT committed
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err = txn2.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Commit txn1, txn2 should still be isolated
	txn1.Commit()

	// Nothing should show up in this transaction
	raw, err = txn2.First("main", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if raw != nil {
		t.Fatalf("bad: %#v", raw)
	}

	// Create a new txn
	txn3 := db.Txn(false)

	// Results should show up in this transaction
	raw, err = txn3.First("main", "id")
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

	err := txn1.Insert("main", toReflectValue(obj))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", toReflectValue(obj2))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = txn1.Insert("main", toReflectValue(obj3))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Abort the txn
	txn1.Abort()
	txn1.Commit()

	// Create a new transaction
	txn2 := db.Txn(false)

	// Nothing should show up in this transaction
	raw, err := txn2.First("main", "id")
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

	// Get using a full name
	raw, err := txn.First("people", "name", "Armon", "Dadgar")
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	raw, err = txn.First("people", "age", uint8(27))
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	raw, err = txn.First("people", "negative_age", int8(-26))
	noErr(t, err)
	if raw == nil {
		t.Fatalf("should get person")
	}

	person := fromReflectValue[TestPerson](raw)
	if person.First != "Armon" {
		t.Fatalf("wrong person!")
	}

	// Where in the world is mitchell hashimoto?
	raw, err = txn.First("people", "name", "Mitchell")
	noErr(t, err)
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
	noErr(t, txn.Insert("people", toReflectValue(person1)))
	noErr(t, txn.Insert("people", toReflectValue(person2)))
	noErr(t, txn.Insert("places", toReflectValue(place1)))
	noErr(t, txn.Insert("places", toReflectValue(place2)))
	noErr(t, txn.Insert("places", toReflectValue(place3)))
	noErr(t, txn.Insert("visits", toReflectValue(visit1)))
	noErr(t, txn.Insert("visits", toReflectValue(visit2)))

	// Commit
	txn.Commit()
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
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

func testComplexSchema() *memdb.DBSchema {
	var person TestPerson
	personNameIndex := indices.NewMultiIndex(
		indices.NewFieldIndex("first", &person, &person.First),
		indices.NewFieldIndex("last", &person, &person.Last),
	)
	personNameSchema := personNameIndex.Schema()
	personNameSchema.Name = "name"
	personAgeIndex := indices.NewFieldIndex("age", &person, &person.Age)
	personNegativeAgeIndex := indices.NewFieldIndex("negative_age", &person, &person.NegativeAge)

	var place TestPlace
	placeNameIndex := indices.NewFieldIndex("name", &place, &place.Name)

	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"people": {
				Name: "people",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: indices.IDIndexer{},
					},
					personNameSchema.Name:         personNameSchema,
					personAgeIndex.Name():         personAgeIndex.Schema(),
					personNegativeAgeIndex.Name(): personNegativeAgeIndex.Schema(),
				},
			},
			"places": {
				Name: "places",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: indices.IDIndexer{},
					},
					placeNameIndex.Name(): placeNameIndex.Schema(),
				},
			},
			"visits": {
				Name: "visits",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: indices.IDIndexer{},
					},
				},
			},
		},
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

func toReflectValue(obj any) reflect.Value {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	v2 := reflect.New(v.Type())
	v2.Elem().Set(v)
	return v2
}

func fromReflectValue[T any](v any) T {
	return v.(reflect.Value).Elem().Interface().(T)
}
