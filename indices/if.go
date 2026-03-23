package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

// IfIndex indexes those elements from another index for which f returns true.
type IfIndex[T any] struct {
	id       uint64
	subIndex Index[T]
	indexer  *ifIndexer[T]
	unique   bool
}

// NewIfIndex creates new conditional index.
func NewIfIndex[T any](subIndex Index[T], f func(o *T) bool) *IfIndex[T] {
	var _ Index[T] = (*IfIndex[T])(nil)

	schema := subIndex.Schema()
	index := &IfIndex[T]{
		subIndex: subIndex,
		indexer: &ifIndexer[T]{
			subIndexer: schema.Indexer,
			f:          f,
			args:       schema.Indexer.Args(),
		},
		unique: schema.Unique,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// ID returns ID of the index.
func (i *IfIndex[T]) ID() uint64 {
	return i.id
}

// Schema returns memdb index schema.
func (i *IfIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  i.unique,
		Indexer: i.indexer,
	}
}

// Type returns type of entity index is created for.
func (i *IfIndex[T]) Type() reflect.Type {
	return reflect.TypeFor[T]()
}

func (i *IfIndex[T]) dummyTDefiner(t T) {
	panic("it should never be called")
}

var _ memdb.Indexer = &ifIndexer[int]{}

type ifIndexer[T any] struct {
	subIndexer memdb.Indexer
	f          func(o *T) bool
	args       []memdb.ArgSerializer
}

func (i *ifIndexer[T]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *ifIndexer[T]) SizeFromObject(o unsafe.Pointer) uint64 {
	if !i.f((*T)(o)) {
		return 0
	}
	return i.subIndexer.SizeFromObject(o)
}

func (i *ifIndexer[T]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	if !i.f((*T)(o)) {
		return 0
	}
	return i.subIndexer.FromObject(b, o)
}
