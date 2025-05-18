// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb_test

import (
	"testing"

	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/id"
	"github.com/outofforest/memdb/indices"
)

func TestDBSchema_Validate(t *testing.T) {
	if _, err := memdb.NewMemDB([][]memdb.Index{}); err == nil {
		t.Fatalf("should not validate, empty")
	}

	if _, err := memdb.NewMemDB(testValidSchema()); err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

func TestIndexSchema_Validate(t *testing.T) {
	s := &memdb.IndexSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexer")
	}

	s.Indexer = id.Indexer{}
	err = s.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

var (
	o        TestObject
	indexFoo = indices.NewFieldIndex(&o, &o.Foo)
)

func testValidSchema() [][]memdb.Index {
	return [][]memdb.Index{
		{
			indexFoo,
		},
	}
}
