package indices

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// MultiIndex compiles many indices into a single one.
type MultiIndex[T any] struct {
	id      uint64
	indexer memdb.Indexer
	unique  bool
}

// NewMultiIndex creates new multiindex.
func NewMultiIndex[T any](subIndices ...Index[T]) *MultiIndex[T] {
	var _ Index[T] = (*MultiIndex[T])(nil)
	var _ memdb.Indexer = (*multiIndexer[T])(nil)

	if len(subIndices) == 0 {
		panic(errors.Errorf("no subindices has been provided"))
	}

	var unique bool
	var args []memdb.ArgSerializer
	subIndexers := make([]memdb.Indexer, 0, len(subIndices))
	for _, si := range subIndices {
		schema := si.Schema()
		subIndexers = append(subIndexers, schema.Indexer)
		unique = unique || schema.Unique
		args = append(args, schema.Indexer.Args()...)
	}

	index := &MultiIndex[T]{
		indexer: &multiIndexer[T]{
			subIndices:  subIndices,
			subIndexers: subIndexers,
			args:        args,
		},
		unique: unique,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// ID returns ID of the index.
func (i *MultiIndex[T]) ID() uint64 {
	return i.id
}

// Schema returns memdb index schema.
func (i *MultiIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  i.unique,
		Indexer: i.indexer,
	}
}

// Type returns type of entity index is created for.
func (i *MultiIndex[T]) Type() reflect.Type {
	return reflect.TypeFor[T]()
}

func (i *MultiIndex[T]) dummyTDefiner(t T) {
	panic("it should never be called")
}

type multiIndexer[T any] struct {
	subIndices  []Index[T]
	subIndexers []memdb.Indexer
	args        []memdb.ArgSerializer
}

func (mi *multiIndexer[T]) Args() []memdb.ArgSerializer {
	return mi.args
}

func (mi *multiIndexer[T]) SizeFromObject(o unsafe.Pointer) uint64 {
	var size uint64
	for _, si := range mi.subIndexers {
		s := si.SizeFromObject(o)
		if s == 0 {
			return 0
		}
		size += s
	}
	return size
}

func (mi *multiIndexer[T]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	var n uint64
	for _, si := range mi.subIndexers {
		n += si.FromObject(b[n:], o)
	}
	return n
}
