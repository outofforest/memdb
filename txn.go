// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"bytes"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/iradix"
	"github.com/outofforest/memdb/tree"
)

// ErrNotFound is returned when the requested item is not found.
var ErrNotFound = errors.Errorf("not found")

// Operator describes the matching algorithm applied to the following arguments.
type Operator uint64

const (
	// From means that following arguments will be used to execute lower bound matching.
	From Operator = iota + 1

	// Back means that the following argument is an integer used to move the iterator back.
	Back
)

// Txn is a transaction against a MemDB.
// This can be a read or write transaction.
type Txn struct {
	db             *MemDB
	write          bool
	rootTxn        *tree.Tree[any]
	oldRootPointer unsafe.Pointer
}

// Commit is used to finalize this transaction.
// This is a noop for read transactions,
// already aborted or committed transactions.
func (txn *Txn) Commit() {
	// Noop for a read transaction.
	if !txn.write {
		panic("commit called on read-only transaction")
	}

	// Update the root of the DB.
	previousRootPointer := atomic.SwapPointer(&txn.db.root, unsafe.Pointer(txn.rootTxn))
	if previousRootPointer != txn.oldRootPointer {
		panic("root pointer has changed during transaction")
	}
}

// Insert is used to add or update an object into the given table.
//
// When updating an object, the obj provided should be a copy rather
// than a value updated in-place. Modifying values in-place that are already
// inserted into MemDB is not supported behavior.
func Insert[T any](txn *Txn, table uint64, obj *T) (*T, error) {
	if table >= uint64(len(txn.db.schema)) {
		return nil, errors.Errorf("invalid table '%d'", table)
	}

	objPtr := unsafe.Pointer(obj)

	// Iterator the table schema
	tableSchema := txn.db.schema[table]

	// Iterator the primary ID of the object
	idSchema := tableSchema[IDIndexID]
	idIndexer := idSchema.Indexer
	id := make([]byte, IDLength)
	idIndexer.FromObject(id, objPtr)

	idTxn := writableIndex[T](txn.rootTxn, idSchema.id)
	previousObj := idTxn.Insert(id, obj)
	var existingPtr unsafe.Pointer
	if previousObj != nil {
		existingPtr = unsafe.Pointer(previousObj)
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

		indexTxn := writableIndex[T](txn.rootTxn, indexSchema.id)

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
func Delete[T any](txn *Txn, table uint64, obj *T) (*T, error) {
	if table >= uint64(len(txn.db.schema)) {
		return nil, errors.Errorf("invalid table '%d'", table)
	}

	objPtr := unsafe.Pointer(obj)

	// Iterator the table schema.
	tableSchema := txn.db.schema[table]

	// Iterator the primary ID of the object.
	idSchema := tableSchema[IDIndexID]
	idIndexer := idSchema.Indexer
	id := make([]byte, IDLength)
	idIndexer.FromObject(id, objPtr)

	idTxn := writableIndex[T](txn.rootTxn, idSchema.id)
	previousObj := idTxn.Delete(id)
	if previousObj == nil {
		return nil, ErrNotFound
	}
	existingPtr := unsafe.Pointer(previousObj)

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

			indexTxn := writableIndex[T](txn.rootTxn, indexSchema.id)

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
func First[T any](txn *Txn, table, index uint64, args ...any) (*T, error) {
	iter, err := getIndexIterator[T](txn, false, table, index, args...)
	if err != nil {
		return nil, err
	}

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
func Iterator[T any](txn *Txn, table, index uint64, args ...any) (ResultIterator[T], error) {
	indexIter, err := getIndexIterator[T](txn, true, table, index, args...)
	if err != nil {
		return nil, err
	}

	// Create an iterator
	iter := &radixIterator[T]{
		iter: indexIter,
	}

	return iter, nil
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
type ResultIterator[T any] interface {
	// Next returns the next result from the iterator. If there are no more results
	// nil is returned.
	Next() *T
}

// radixIterator is used to wrap an underlying iradix iterator.
// This is much more efficient than a sliceIterator as we are not
// materializing the entire view.
type radixIterator[T any] struct {
	iter *iradix.Iterator[T]
}

func (r *radixIterator[T]) Next() *T {
	return r.iter.Next()
}

// readableIndex returns a transaction usable for reading the given index in a
// table. If the transaction is a write transaction with modifications, a clone of the
// modified index will be returned.
func readableIndex[T any](rootTxn *tree.Tree[any], indexID uint64, clone bool) *iradix.Txn[T] {
	indexAny, dirty := rootTxn.Get(indexID)
	index := indexAny.(*iradix.Txn[T])
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
func writableIndex[T any](rootTxn *tree.Tree[any], indexID uint64) *iradix.Txn[T] {
	indexAny, dirty := rootTxn.Get(indexID)
	index := indexAny.(*iradix.Txn[T])
	if !dirty {
		index = iradix.NewTxn(index.Root())
		rootTxn.Set(indexID, index)
	}
	return index
}

func getIndexIterator[T any](
	txn *Txn,
	clone bool,
	table, index uint64,
	args ...any,
) (*iradix.Iterator[T], error) {
	if table >= uint64(len(txn.db.schema)) {
		return nil, errors.Errorf("invalid table '%d'", table)
	}
	// Iterator the table schema.
	tableSchema := txn.db.schema[table]

	// Iterator the index schema.
	indexSchema, ok := tableSchema[index]
	if !ok {
		return nil, errors.Errorf("invalid index '%d'", index)
	}

	// Iterator the exact match index.
	argDefs := indexSchema.Indexer.Args()

	var numOfArgs int
	var keySize uint64
	for i, a := range args {
		if op, ok := a.(Operator); ok {
			if op == Back {
				if len(args) != i+2 {
					return nil, errors.New("invalid argument count")
				}
				break
			}
			continue
		}
		if numOfArgs == len(argDefs) {
			return nil, errors.Errorf("too many arguments, received: %d, acceptable: %d", len(args),
				len(argDefs))
		}
		keySize += argDefs[numOfArgs].SizeFromArg(a)
		numOfArgs++
	}

	indexTxn := readableIndex[T](txn.rootTxn, indexSchema.id, clone)
	indexRoot := indexTxn.Root()

	// Iterator an iterator over the index.
	indexIter := indexRoot.Iterator()

	if numOfArgs == 0 {
		return indexIter, nil
	}
	if keySize == 0 {
		return nil, errors.Errorf("empty key")
	}

	key := make([]byte, keySize)
	fromArgs := keySize
	var backCount uint64
	var lastOperator Operator
	var n uint64
	var argI int

loop:
	for i, a := range args {
		if op, ok := a.(Operator); ok {
			if op <= lastOperator {
				return nil, errors.New("invalid operator")
			}

			switch op {
			case From:
				fromArgs = n
			case Back:
				if count, ok := args[i+1].(int); ok {
					backCount = uint64(count)
					break loop
				}
				count, ok := args[i+1].(uint64)
				if !ok {
					return nil, errors.New("invalid count")
				}
				backCount = count
				break loop
			default:
				return nil, errors.New("invalid operator")
			}
			continue
		}
		n += argDefs[argI].FromArg(key[n:], a)
		argI++
	}

	if fromArgs > 0 {
		indexIter.SeekPrefix(key[:fromArgs])
	}
	if fromArgs < uint64(len(key)) {
		indexIter.SeekLowerBound(key[fromArgs:])
	}
	if backCount > 0 {
		indexIter.Back(backCount)
	}
	return indexIter, nil
}
