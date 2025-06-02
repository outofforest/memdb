package indices

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// NewIfIndex creates new conditional index.
func NewIfIndex[T any](subIndex memdb.Index, f func(o *T) bool) *IfIndex[T] {
	var v T
	if t := reflect.TypeOf(v); t != subIndex.Type() {
		panic(errors.Errorf("subindex type mismatch, expected: %s, got: %s", t, subIndex.Type()))
	}

	schema := subIndex.Schema()
	index := &IfIndex[T]{
		subIndex: subIndex,
		indexer: ifIndexer[T]{
			subIndexer: schema.Indexer,
			f:          f,
		},
		unique: schema.Unique,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// IfIndex indexes those elements from another index for which f returns true.
type IfIndex[T any] struct {
	id       uint64
	subIndex memdb.Index
	indexer  memdb.Indexer
	unique   bool
}

// ID returns ID of the index.
func (i *IfIndex[T]) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *IfIndex[T]) Type() reflect.Type {
	return i.subIndex.Type()
}

// Schema returns memdb index schema.
func (i *IfIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  i.unique,
		Indexer: i.indexer,
	}
}

var _ memdb.Indexer = ifIndexer[int]{}

type ifIndexer[T any] struct {
	subIndexer memdb.Indexer
	f          func(o *T) bool
}

func (i ifIndexer[T]) Args() []memdb.ArgSerializer {
	return i.subIndexer.Args()
}

func (i ifIndexer[T]) SizeFromObject(o unsafe.Pointer) uint64 {
	if !i.f((*T)(o)) {
		return 0
	}
	return i.subIndexer.SizeFromObject(o)
}

func (i ifIndexer[T]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	if !i.f((*T)(o)) {
		return 0
	}
	return i.subIndexer.FromObject(b, o)
}
