// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/outofforest/iradix"
	"github.com/outofforest/memdb/id"
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
	root   unsafe.Pointer // *iradix.Tree underneath

	// There can only be a single writer at once
	writer sync.Mutex
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(indexes [][]Index) (*MemDB, error) {
	schema := make(DBSchema, 0, len(indexes))

	var indexID uint64
	var indexIDBytes [8]byte
	for _, tableIndexes := range indexes {
		t := TableSchema{}
		schema = append(schema, t)

		t[id.IndexID] = &IndexSchema{
			Unique:  true,
			Indexer: id.Indexer{},
		}

		for _, index := range tableIndexes {
			indexSchema := index.Schema()
			t[index.ID()] = indexSchema

			indexID++
			binary.BigEndian.PutUint64(indexIDBytes[:], indexID)
			for j, b := range indexIDBytes {
				if b != 0 {
					indexSchema.id = make([]byte, len(indexIDBytes[j:]))
					copy(indexSchema.id, indexIDBytes[j:])
					break
				}
			}
		}
	}

	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema: schema,
		root:   unsafe.Pointer(iradix.New()),
	}
	db.initialize()
	return db, nil
}

// DBSchema returns schema in use for introspection.
//
// The method is intended for *read-only* debugging use cases,
// returned schema should *never be modified in-place*.
func (db *MemDB) DBSchema() DBSchema {
	return db.schema
}

// getRoot is used to do an atomic load of the root pointer.
func (db *MemDB) getRoot() *iradix.Node {
	return (*iradix.Node)(atomic.LoadPointer(&db.root))
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: iradix.NewTxn(db.getRoot()),
	}
	return txn
}

// Snapshot is used to capture a point-in-time snapshot  of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		schema: db.schema,
		root:   unsafe.Pointer(db.getRoot()),
	}
	return clone
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() {
	txn := iradix.NewTxn(db.getRoot())
	for _, tableSchema := range db.schema {
		for _, i := range tableSchema {
			index := iradix.New()
			txn.Insert(i.id, index)
		}
	}
	db.root = unsafe.Pointer(txn.Commit())
}
