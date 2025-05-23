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
	memdbid "github.com/outofforest/memdb/id"
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

	// Get the table schema
	tableSchema := txn.db.schema[table]

	// Get the primary ID of the object
	idSchema := tableSchema[memdbid.IndexID]
	idIndexer := idSchema.Indexer
	id := make([]byte, memdbid.Length)
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
		if indexID == memdbid.IndexID {
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

	// Get the table schema.
	tableSchema := txn.db.schema[table]

	// Get the primary ID of the object.
	idSchema := tableSchema[memdbid.IndexID]
	idIndexer := idSchema.Indexer
	id := make([]byte, memdbid.Length)
	idIndexer.FromObject(id, objPtr)

	idTxn := txn.writableIndex(idSchema.id)
	previousObj := idTxn.Delete(id)
	if previousObj == nil {
		return nil, ErrNotFound
	}
	existingPtr := previousObj.UnsafePointer()

	// Remove the object from all the indexes.
	for indexID, indexSchema := range tableSchema {
		if indexID == memdbid.IndexID {
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
	// Get the index value
	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Get the index itself
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

// Last is used to return the last matching object for
// the given constraints on the index.
//
// Note that all values read in the transaction form a consistent snapshot
// from the time when the transaction was created.
func (txn *Txn) Last(table, index uint64, args ...any) (*reflect.Value, error) {
	// Get the index value
	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Get the index itself
	indexTxn := txn.readableIndex(indexSchema.id, false)

	// Do an exact lookup
	if indexSchema.Unique && val != nil {
		return indexTxn.Get(val), nil
	}

	// Handle non-unique index by using an iterator and getting the last value
	iter := indexTxn.Root().ReverseIterator()
	iter.SeekPrefix(val)
	return iter.Previous(), nil
}

// Get is used to construct a ResultIterator over all the rows that match the
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
func (txn *Txn) Get(table, index uint64, args ...any) (ResultIterator, error) {
	indexIter, val, err := txn.getIndexIterator(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Seek the iterator to the appropriate sub-set
	indexIter.SeekPrefix(val)

	// Create an iterator
	iter := &radixIterator{
		iter: indexIter,
	}

	return iter, nil
}

// GetReverse is used to construct a Reverse ResultIterator over all the
// rows that match the given constraints of an index.
// The returned ResultIterator's Next() will return the next Previous value.
//
// See the documentation on Get for details on arguments.
//
// See the documentation for ResultIterator to understand the behaviour of the
// returned ResultIterator.
func (txn *Txn) GetReverse(table, index uint64, args ...any) (ResultIterator, error) {
	indexIter, val, err := txn.getIndexIteratorReverse(table, index, args...)
	if err != nil {
		return nil, err
	}

	// Seek the iterator to the appropriate sub-set
	indexIter.SeekPrefix(val)

	// Create an iterator
	iter := &radixReverseIterator{
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

// getIndexValue is used to get the IndexSchema and the value
// used to scan the index given the parameters.
func (txn *Txn) getIndexValue(table, index uint64, args ...any) (*IndexSchema, []byte, error) {
	if table >= uint64(len(txn.db.schema)) {
		return nil, nil, errors.Errorf("invalid table '%d'", table)
	}
	// Get the table schema.
	tableSchema := txn.db.schema[table]

	// Get the index schema.
	indexSchema, ok := tableSchema[index]
	if !ok {
		return nil, nil, errors.Errorf("invalid index '%d'", index)
	}

	// Hot-path for when there are no arguments.
	if len(args) == 0 {
		return indexSchema, nil, nil
	}

	// Get the exact match index.
	indexer := indexSchema.Indexer
	keySize := indexer.SizeFromArgs(args...)
	if keySize == 0 {
		return indexSchema, nil, errors.Errorf("empty key")
	}

	b := make([]byte, keySize)
	indexer.FromArgs(b, args...)
	return indexSchema, b, nil
}

func (txn *Txn) getIndexIterator(table, index uint64, args ...any) (*iradix.Iterator[reflect.Value], []byte, error) {
	// Get the index value to scan.
	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, nil, err
	}

	// Get the index itself.
	indexTxn := txn.readableIndex(indexSchema.id, true)
	indexRoot := indexTxn.Root()

	// Get an iterator over the index.
	indexIter := indexRoot.Iterator()
	return indexIter, val, nil
}

func (txn *Txn) getIndexIteratorReverse(
	table, index uint64,
	args ...any,
) (*iradix.ReverseIterator[reflect.Value], []byte, error) {
	// Get the index value to scan.
	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, nil, err
	}

	// Get the index itself.
	indexTxn := txn.readableIndex(indexSchema.id, true)
	indexRoot := indexTxn.Root()

	// Get an iterator over the index.
	indexIter := indexRoot.ReverseIterator()
	return indexIter, val, nil
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

type radixReverseIterator struct {
	iter *iradix.ReverseIterator[reflect.Value]
}

func (r *radixReverseIterator) Next() *reflect.Value {
	return r.iter.Previous()
}
