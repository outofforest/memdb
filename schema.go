// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
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

		if _, ok := indexes[IDIndexID]; !ok {
			return errors.Errorf("table %d must have id index", tableID)
		}

		if !indexes[IDIndexID].Unique {
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
	Schema() *IndexSchema
}

// ArgSerializerIndexer combines ArgSerializer and Indexer.
type ArgSerializerIndexer interface {
	ArgSerializer
	Indexer
}

// ArgSerializer serializes index argument.
type ArgSerializer interface {
	SizeFromArg(arg any) uint64
	FromArg(b []byte, args any) uint64
}

// Indexer is an interface used for defining indexes.
type Indexer interface {
	// Args returns arg serializer for index.
	Args() []ArgSerializer

	// SizeFromObject returns byte size of the index key based on the object.
	SizeFromObject(o unsafe.Pointer) uint64

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
