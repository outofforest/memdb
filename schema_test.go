// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"testing"

	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
)

func TestDBSchema_Validate(t *testing.T) {
	s := &memdb.DBSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	s.Tables = map[string]*memdb.TableSchema{
		"foo": {Name: "foo"},
	}
	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexes")
	}

	valid := testValidSchema()
	err = valid.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

func TestTableSchema_Validate(t *testing.T) {
	s := &memdb.TableSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	s.Indexes = map[string]*memdb.IndexSchema{
		"foo": {Name: "foo"},
	}
	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexes")
	}

	valid := &memdb.TableSchema{
		Name: "main",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: indices.IDIndexer{},
			},
		},
	}
	err = valid.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

func TestIndexSchema_Validate(t *testing.T) {
	s := &memdb.IndexSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	s.Name = "foo"
	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexer")
	}

	s.Indexer = indices.IDIndexer{}
	err = s.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

func testValidSchema() *memdb.DBSchema {
	var o TestObject
	indexFoo := indices.NewFieldIndex("foo", &o, &o.Foo)

	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"main": {
				Name: "main",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: indices.IDIndexer{},
					},
					indexFoo.Name(): indexFoo.Schema(),
				},
			},
		},
	}
}
