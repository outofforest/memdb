// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb/id"
)

// DBSchema is the schema to use for the full database with a MemDB instance.
//
// MemDB will require a valid schema. Schema validation can be tested using
// the Validate function. Calling this function is recommended in unit tests.
type DBSchema []TableSchema

// TableSchema contains indexes.
type TableSchema map[uint64]*IndexSchema

// Validate validates the schema.
func (s DBSchema) Validate() error {
	if len(s) == 0 {
		return errors.New("schema is empty")
	}

	if len(s) == 0 {
		return errors.New("schema has no tables defined")
	}

	for tableID, indexes := range s {
		if len(indexes) == 0 {
			return errors.Errorf("missing table indexes for %d", tableID)
		}

		if _, ok := indexes[id.IndexID]; !ok {
			return errors.Errorf("table %d must have id index", tableID)
		}

		if !indexes[id.IndexID].Unique {
			return errors.Errorf("id index of table %d must be unique", tableID)
		}

		for name, index := range indexes {
			if err := index.Validate(); err != nil {
				return errors.Errorf("index %q: %s", name, err)
			}
		}
	}

	return nil
}

// Index defines the interface of index.
type Index interface {
	ID() uint64
	Type() reflect.Type
	NumOfArgs() uint64
	Schema() *IndexSchema
}

// Indexer is an interface used for defining indexes.
type Indexer interface {
	// SizeFromObject returns byte size of the index key based on the object.
	SizeFromObject(o unsafe.Pointer) uint64

	// SizeFromArgs returns byte size of the index key based on the args.
	SizeFromArgs(args ...any) uint64

	// FromArgs is called to build the exact index key from a list of arguments.
	FromArgs(b []byte, args ...any) uint64

	// FromObject extracts the index value from an object.
	FromObject(b []byte, o unsafe.Pointer) uint64
}

// IndexSchema is the schema for an index. An index defines how a table is
// queried.
type IndexSchema struct {
	Unique  bool
	Indexer Indexer

	id uint64
}

// Validate validates schema.
func (s *IndexSchema) Validate() error {
	if s.Indexer == nil {
		return errors.New("missing index function")
	}
	return nil
}
