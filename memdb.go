// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/outofforest/iradix"
	"github.com/outofforest/memdb/tree"
)

// Config is the memdb config.
type Config struct {
	Entities []reflect.Type
	Indices  []Index
}

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
	schema dbSchema
	root   unsafe.Pointer // *tree.Tree underneath
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(config Config) (*MemDB, error) {
	indicesByEntity := map[reflect.Type][]Index{}
	for _, eType := range config.Entities {
		if _, exists := indicesByEntity[eType]; exists {
			return nil, fmt.Errorf("duplicated entity %s", eType)
		}
		indicesByEntity[eType] = nil
	}

	for _, i := range config.Indices {
		t := i.Type()
		if _, exists := indicesByEntity[t]; !exists {
			return nil, fmt.Errorf("index for undefined entity %s", t)
		}
		indicesByEntity[t] = append(indicesByEntity[t], i)
	}

	root := tree.New[*iradix.Txn[unsafe.Pointer]]()
	db := &MemDB{
		schema: make(dbSchema, 0, len(indicesByEntity)),
		root:   unsafe.Pointer(root),
	}

	var indexID uint64
	for _, eT := range config.Entities {
		t := tableSchema{}
		db.schema = append(db.schema, t)

		indexID++
		t[IDIndexID] = &IndexSchema{
			Unique:  true,
			Indexer: IDIndexer{},
			id:      indexID,
		}
		root.Set(indexID, iradix.NewTxn(iradix.New[unsafe.Pointer]()))

		for _, index := range indicesByEntity[eT] {
			indexID++
			indexSchema := index.Schema()
			indexSchema.id = indexID
			t[index.ID()] = indexSchema
			root.Set(indexID, iradix.NewTxn(iradix.New[unsafe.Pointer]()))
		}
	}

	// Validate the schema
	if err := db.schema.Validate(); err != nil {
		return nil, err
	}

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

// getRoot is used to do an atomic load of the root pointer.
func (db *MemDB) getRoot() (*tree.Tree[*iradix.Txn[unsafe.Pointer]], unsafe.Pointer) {
	pointer := atomic.LoadPointer(&db.root)
	return (*tree.Tree[*iradix.Txn[unsafe.Pointer]])(pointer), pointer
}
