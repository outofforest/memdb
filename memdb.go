// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/outofforest/iradix"
	"github.com/outofforest/memdb/tree"
)

// MemDB is an in-memory database providing Atomicity, Consistency, and
// Isolation from ACID. MemDB doesn't provide Durability since it is an
// in-memory database.
//
// MemDB provides a table abstraction to store objects (rows) with multiple
// indexes based on inserted values. The database makes use of immutable radix
// trees to provide transactions and MVCC.
//
// Objects inserted into MemDB are not copied. It is **extremely important**
// that objects are not modified in-place after they are inserted since they
// are stored directly in MemDB. It remains unsafe to modify inserted objects
// even after they've been deleted from MemDB since there may still be older
// snapshots of the DB being read from other goroutines.
type MemDB struct {
	schema DBSchema
	root   unsafe.Pointer // *tree.Tree underneath
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(indexes [][]Index) (*MemDB, error) {
	schema := make(DBSchema, 0, len(indexes))

	for _, tableIndexes := range indexes {
		t := TableSchema{}
		schema = append(schema, t)

		t[IDIndexID] = &IndexSchema{
			Unique:  true,
			Indexer: IDIndexer{},
		}

		for _, index := range tableIndexes {
			indexSchema := index.Schema()
			t[index.ID()] = indexSchema
		}
	}

	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema: schema,
		root:   unsafe.Pointer(tree.New[iradix.Txn[reflect.Value]]()),
	}
	db.initialize()
	return db, nil
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	root, rootPointer := db.getRoot()
	return &Txn{
		db:             db,
		write:          write,
		rootTxn:        root.Next(),
		oldRootPointer: rootPointer,
	}
}

// Snapshot is used to capture a point-in-time snapshot of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	_, rootPointer := db.getRoot()
	return &MemDB{
		schema: db.schema,
		root:   rootPointer,
	}
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() {
	root, _ := db.getRoot()
	var indexID uint64
	for _, tableSchema := range db.schema {
		for _, indexSchema := range tableSchema {
			indexID++
			indexSchema.id = indexID
			root.Set(indexID, iradix.NewTxn(iradix.New[reflect.Value]()))
		}
	}
}

// getRoot is used to do an atomic load of the root pointer.
func (db *MemDB) getRoot() (*tree.Tree[iradix.Txn[reflect.Value]], unsafe.Pointer) {
	pointer := atomic.LoadPointer(&db.root)
	return (*tree.Tree[iradix.Txn[reflect.Value]])(pointer), pointer
}
