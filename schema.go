// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"github.com/pkg/errors"
)

// DBSchema is the schema to use for the full database with a MemDB instance.
//
// MemDB will require a valid schema. Schema validation can be tested using
// the Validate function. Calling this function is recommended in unit tests.
type DBSchema struct {
	// Tables is the set of tables within this database. The key is the
	// table name and must match the Name in TableSchema.
	Tables map[string]*TableSchema
}

// Validate validates the schema.
func (s *DBSchema) Validate() error {
	if s == nil {
		return errors.Errorf("schema is nil")
	}

	if len(s.Tables) == 0 {
		return errors.Errorf("schema has no tables defined")
	}

	for name, table := range s.Tables {
		if name != table.Name {
			return errors.Errorf("table name mis-match for '%s'", name)
		}

		if err := table.Validate(); err != nil {
			return errors.Errorf("table %q: %s", name, err)
		}
	}

	return nil
}

// TableSchema is the schema for a single table.
type TableSchema struct {
	// Name of the table. This must match the key in the Tables map in DBSchema.
	Name string

	// Indexes is the set of indexes for querying this table. The key
	// is a unique name for the index and must match the Name in the
	// IndexSchema.
	Indexes map[string]*IndexSchema
}

// Validate is used to validate the table schema.
func (s *TableSchema) Validate() error {
	if s.Name == "" {
		return errors.Errorf("missing table name")
	}

	if len(s.Indexes) == 0 {
		return errors.Errorf("missing table indexes for '%s'", s.Name)
	}

	if _, ok := s.Indexes["id"]; !ok {
		return errors.Errorf("must have id index")
	}

	if !s.Indexes["id"].Unique {
		return errors.Errorf("id index must be unique")
	}

	for name, index := range s.Indexes {
		if name != index.Name {
			return errors.Errorf("index name mis-match for '%s'", name)
		}

		if err := index.Validate(); err != nil {
			return errors.Errorf("index %q: %s", name, err)
		}
	}

	return nil
}

// Indexer is an interface used for defining indexes.
type Indexer interface {
	// SizeFromObject returns byte size of the index key based on the object.
	SizeFromObject(o any) uint64

	// SizeFromArgs returns byte size of the index key based on the args.
	SizeFromArgs(args ...any) uint64

	// FromArgs is called to build the exact index key from a list of arguments.
	FromArgs(b []byte, args ...any) uint64

	// FromObject extracts the index value from an object.
	FromObject(b []byte, o any) uint64
}

// IndexSchema is the schema for an index. An index defines how a table is
// queried.
type IndexSchema struct {
	// Name of the index. This must be unique among a tables set of indexes.
	// This must match the key in the map of Indexes for a TableSchema.
	Name string

	Unique  bool
	Indexer Indexer
}

// Validate validates schema.
func (s *IndexSchema) Validate() error {
	if s.Name == "" {
		return errors.Errorf("missing index name")
	}
	if s.Indexer == nil {
		return errors.Errorf("missing index function for '%s'", s.Name)
	}
	return nil
}
