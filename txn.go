// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"bytes"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/iradix"
	"github.com/outofforest/memdb/tree"
)

// ErrNotFound is returned when the requested item is not found.
var ErrNotFound = errors.Errorf("not found")

// Txn is a transaction against a MemDB.
// This can be a read or write transaction.
type Txn struct {
	db      *MemDB
	write   bool
	rootTxn *tree.Tree[iradix.Txn[reflect.Value]]
}

// Abort is used to cancel this transaction.
// This is a noop for read transactions,
// already aborted or committed transactions.
func (txn *Txn) Abort() {
	// Noop for a read transaction
	if !txn.write {
		return
	}

	// Check if already aborted or committed
	if txn.rootTxn == nil {
		return
	}

	// Clear the txn
	txn.rootTxn = nil

	// Release the writer lock since this is invalid
	txn.db.writer.Unlock()
}

// Commit is used to finalize this transaction.
// This is a noop for read transactions,
// already aborted or committed transactions.
func (txn *Txn) Commit() {
	// Noop for a read transaction.
	if !txn.write {
		return
	}

	// Check if already aborted or committed.
	if txn.rootTxn == nil {
		return
	}

	// Update the root of the DB.
	atomic.StorePointer(&txn.db.root, unsafe.Pointer(txn.rootTxn))

	// Clear the txn.
	txn.rootTxn = nil

	// Release the writer lock since this is invalid.
	txn.db.writer.Unlock()
}

// Insert is used to add or update an object into the given table.
//
// When updating an object, the obj provided should be a copy rather
// than a value updated in-place. Modifying values in-place that are already
// inserted into MemDB is not supported behavior.
func (txn *Txn) Insert(table uint64, obj *reflect.Value) (*reflect.Value, error) {
	if !txn.write {
		return nil, errors.Errorf("cannot insert in read-only transaction")
	}
	if table >= uint64(len(txn.db.schema)) {
		return nil, errors.Errorf("invalid table '%d'", table)
	}
	if obj.Kind() != reflect.Ptr {
		return nil, errors.Errorf("non-pointer object '%v'", obj.Type())
	}

	objPtr := obj.UnsafePointer()

	// Iterator the table schema
	tableSchema := txn.db.schema[table]

	// Iterator the primary ID of the object
	idSchema := tableSchema[IDIndexID]
	idIndexer := idSchema.Indexer
	id := make([]byte, IDLength)
	idIndexer.FromObject(id, objPtr)

	idTxn := txn.writableIndex(idSchema.id)
	previousObj := idTxn.Insert(id, obj)
	var existingPtr unsafe.Pointer
	if previousObj != nil {
		existingPtr = previousObj.UnsafePointer()
	}

	// On an update, there is an existing object with the given
	// primary ID. We do the update by deleting the current object
	// and inserting the new object.
	for indexID, indexSchema := range tableSchema {
		if indexID == IDIndexID {
			continue
		}

		indexer := indexSchema.Indexer
		keySize := indexer.SizeFromObject(objPtr)
		var b []byte
		var n uint64
		if keySize > 0 {
			if !indexSchema.Unique {
				keySize += uint64(len(id))
			}

			b = make([]byte, keySize)
			n = indexSchema.Indexer.FromObject(b, objPtr)

			// Handle non-unique index by computing a unique index.
			// This is done by appending the primary key which must
			// be unique anyway.
			if !indexSchema.Unique {
				copy(b[n:], id)
			}
		}

		indexTxn := txn.writableIndex(indexSchema.id)

		// Handle the update by deleting from the index first
		//nolint:nestif
		if previousObj != nil {
			if keySize := indexer.SizeFromObject(existingPtr); keySize > 0 {
				if !indexSchema.Unique {
					keySize += uint64(len(id))
				}

				existingB := make([]byte, keySize)
				existingN := indexer.FromObject(existingB, existingPtr)

				// If we are writing to the same index with the same value,
				// we can avoid the delete as the insert will overwrite the
				// value anyway.
				if b == nil || !bytes.Equal(existingB[:existingN], b[:n]) {
					// Handle non-unique index by computing a unique index.
					// This is done by appending the primary key which must
					// be unique anyways.
					if !indexSchema.Unique {
						copy(existingB[existingN:], id)
					}

					indexTxn.Delete(existingB)
				}
			}
		}

		// Update the value of the index
		if b != nil {
			indexTxn.Insert(b, obj)
		}
	}
	return previousObj, nil
}

// Delete is used to delete a single object from the given table.
// This object must already exist in the table.
func (txn *Txn) Delete(table uint64, obj *reflect.Value) (*reflect.Value, error) {
	if !txn.write {
		return nil, errors.Errorf("cannot delete in read-only transaction")
	}
	if table >= uint64(len(txn.db.schema)) {
		return nil, errors.Errorf("invalid table '%d'", table)
	}
	if obj.Kind() != reflect.Ptr {
		return nil, errors.Errorf("non-pointer object '%v'", obj.Type())
	}

	objPtr := obj.UnsafePointer()

	// Iterator the table schema.
	tableSchema := txn.db.schema[table]

	// Iterator the primary ID of the object.
	idSchema := tableSchema[IDIndexID]
	idIndexer := idSchema.Indexer
	id := make([]byte, IDLength)
	idIndexer.FromObject(id, objPtr)

	idTxn := txn.writableIndex(idSchema.id)
	previousObj := idTxn.Delete(id)
	if previousObj == nil {
		return nil, ErrNotFound
	}
	existingPtr := previousObj.UnsafePointer()

	// Remove the object from all the indexes.
	for indexID, indexSchema := range tableSchema {
		if indexID == IDIndexID {
			continue
		}

		indexer := indexSchema.Indexer
		if keySize := indexer.SizeFromObject(existingPtr); keySize > 0 {
			if !indexSchema.Unique {
				keySize += uint64(len(id))
			}

			existingB := make([]byte, keySize)
			existingN := indexer.FromObject(existingB, existingPtr)

			// Handle non-unique index by computing a unique index.
			// This is done by appending the primary key which must
			// be unique anyways.
			if !indexSchema.Unique {
				copy(existingB[existingN:], id)
			}

			indexTxn := txn.writableIndex(indexSchema.id)

			indexTxn.Delete(existingB)
		}
	}
	return previousObj, nil
}

// First is used to return the first matching object for
// the given constraints on the index.
//
// Note that all values read in the transaction form a consistent snapshot
// from the time when the transaction was created.
func (txn *Txn) First(table, index uint64, args ...any) (*reflect.Value, error) {
	// Iterator the index value
	indexSchema, val, _, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Iterator the index itself
	indexTxn := txn.readableIndex(indexSchema.id, false)

	// Do an exact lookup
	if indexSchema.Unique && val != nil {
		return indexTxn.Get(val), nil
	}

	// Handle non-unique index by using an iterator and getting the first value
	iter := indexTxn.Root().Iterator()
	iter.SeekPrefix(val)
	return iter.Next(), nil
}

// Iterator is used to construct a ResultIterator over all the rows that match the
// given constraints of an index. The index values must match exactly (this
// is not a range-based or prefix-based lookup) by default.
//
// Prefix lookups: if the named index implements PrefixIndexer, you may perform
// prefix-based lookups by appending "_prefix" to the index name. In this
// scenario, the index values given in args are treated as prefix lookups. For
// example, a StringFieldIndex will match any string with the given value
// as a prefix: "mem" matches "memdb".
//
// See the documentation for ResultIterator to understand the behaviour of the
// returned ResultIterator.
func (txn *Txn) Iterator(table, index uint64, args ...any) (ResultIterator, error) {
	indexIter, err := txn.getIndexIterator(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Create an iterator
	iter := &radixIterator{
		iter: indexIter,
	}

	return iter, nil
}

// readableIndex returns a transaction usable for reading the given index in a
// table. If the transaction is a write transaction with modifications, a clone of the
// modified index will be returned.
func (txn *Txn) readableIndex(indexID uint64, clone bool) *iradix.Txn[reflect.Value] {
	index, dirty := txn.rootTxn.Get(indexID)
	if dirty {
		if clone {
			return index.Clone()
		}
		return index
	}
	return iradix.NewTxn(index.Root())
}

// writableIndex returns a transaction usable for modifying the
// given index in a table.
func (txn *Txn) writableIndex(indexID uint64) *iradix.Txn[reflect.Value] {
	index, dirty := txn.rootTxn.Get(indexID)
	if !dirty {
		index = iradix.NewTxn(index.Root())
		txn.rootTxn.Set(indexID, index)
	}
	return index
}

// Operator describes the matching algorithm applied to the following arguments.
type Operator int

// From means that following arguments will be used to execute lower bound matching.
const From Operator = iota

// getIndexValue is used to get the IndexSchema and the value
// used to scan the index given the parameters.
func (txn *Txn) getIndexValue(table, index uint64, args ...any) (*IndexSchema, []byte, uint64, error) {
	if table >= uint64(len(txn.db.schema)) {
		return nil, nil, 0, errors.Errorf("invalid table '%d'", table)
	}
	// Iterator the table schema.
	tableSchema := txn.db.schema[table]

	// Iterator the index schema.
	indexSchema, ok := tableSchema[index]
	if !ok {
		return nil, nil, 0, errors.Errorf("invalid index '%d'", index)
	}

	// Iterator the exact match index.
	argDefs := indexSchema.Indexer.Args()

	var numOfArgs int
	var keySize uint64
	for _, a := range args {
		if _, ok := a.(Operator); ok {
			continue
		}
		if numOfArgs == len(argDefs) {
			return nil, nil, 0, errors.Errorf("too many arguments, received: %d, acceptable: %d", len(args),
				len(argDefs))
		}
		keySize += argDefs[numOfArgs].SizeFromArg(a)
		numOfArgs++
	}

	if numOfArgs == 0 {
		return indexSchema, nil, 0, nil
	}
	if keySize == 0 {
		return indexSchema, nil, 0, errors.Errorf("empty key")
	}

	b := make([]byte, keySize)
	splitIndex := keySize
	var n uint64
	var argI int
	for _, a := range args {
		if _, ok := a.(Operator); ok {
			splitIndex = n
			continue
		}
		n += argDefs[argI].FromArg(b[n:], a)
		argI++
	}

	return indexSchema, b, splitIndex, nil
}

func (txn *Txn) getIndexIterator(table, index uint64, args ...any) (*iradix.Iterator[reflect.Value], error) {
	// Iterator the index value to scan.
	indexSchema, val, splitIndex, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Iterator the index itself.
	indexTxn := txn.readableIndex(indexSchema.id, true)
	indexRoot := indexTxn.Root()

	// Iterator an iterator over the index.
	indexIter := indexRoot.Iterator()
	if splitIndex > 0 {
		indexIter.SeekPrefix(val[:splitIndex])
	}
	if splitIndex < uint64(len(val)) {
		indexIter.SeekLowerBound(val[splitIndex:])
	}
	return indexIter, nil
}

// ResultIterator is used to iterate over a list of results from a query on a table.
//
// When a ResultIterator is created from a write transaction, the results from
// Next will reflect a snapshot of the table at the time the ResultIterator is
// created.
// This means that calling Insert or Delete on a transaction while iterating is
// allowed, but the changes made by Insert or Delete will not be observed in the
// results returned from subsequent calls to Next. For example if an item is deleted
// from the index used by the iterator it will still be returned by Next. If an
// item is inserted into the index used by the iterator, it will not be returned
// by Next. However, an iterator created after a call to Insert or Delete will
// reflect the modifications.
//
// When a ResultIterator is created from a write transaction, and there are already
// modifications to the index used by the iterator, the modification cache of the
// index will be invalidated. This may result in some additional allocations if
// the same node in the index is modified again.
type ResultIterator interface {
	// Next returns the next result from the iterator. If there are no more results
	// nil is returned.
	Next() *reflect.Value
}

// radixIterator is used to wrap an underlying iradix iterator.
// This is much more efficient than a sliceIterator as we are not
// materializing the entire view.
type radixIterator struct {
	iter *iradix.Iterator[reflect.Value]
}

func (r *radixIterator) Next() *reflect.Value {
	return r.iter.Next()
}
