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

	index := &IfIndex[T]{
		subIndex: subIndex,
		indexer: ifIndexer[T]{
			subIndexer: subIndex.Schema().Indexer,
			f:          f,
		},
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

var _ memdb.Indexer = ifIndexer[int]{}

// IfIndex indexes those elements from another index for which f returns true.
type IfIndex[T any] struct {
	id       uint64
	subIndex memdb.Index
	indexer  memdb.Indexer
}

// ID returns ID of the index.
func (i *IfIndex[T]) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *IfIndex[T]) Type() reflect.Type {
	return i.subIndex.Type()
}

// NumOfArgs returns number of arguments taken by the index.
func (i *IfIndex[T]) NumOfArgs() uint64 {
	return i.subIndex.NumOfArgs()
}

// Schema returns memdb index schema.
func (i *IfIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Indexer: i.indexer,
	}
}

type ifIndexer[T any] struct {
	subIndexer memdb.Indexer
	f          func(o *T) bool
}

func (ii ifIndexer[T]) SizeFromObject(o any) uint64 {
	if !ii.f(o.(reflect.Value).Interface().(*T)) {
		return 0
	}
	return ii.subIndexer.SizeFromObject(o)
}

func (ii ifIndexer[T]) SizeFromArgs(args ...any) uint64 {
	return ii.subIndexer.SizeFromArgs(args...)
}

func (ii ifIndexer[T]) FromArgs(b []byte, args ...any) uint64 {
	return ii.subIndexer.FromArgs(b, args...)
}

func (ii ifIndexer[T]) FromObject(b []byte, o any) uint64 {
	if !ii.f(o.(reflect.Value).Interface().(*T)) {
		return 0
	}
	return ii.subIndexer.FromObject(b, o)
}
