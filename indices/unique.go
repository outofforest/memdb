package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

// UniqueIndex marks the subindex definition as unique.
type UniqueIndex[T any] struct {
	id       uint64
	subIndex Index[T]
	indexer  memdb.Indexer
}

// NewUniqueIndex creates new unique index.
func NewUniqueIndex[T any](subIndex Index[T]) *UniqueIndex[T] {
	var _ Index[T] = (*UniqueIndex[T])(nil)

	index := &UniqueIndex[T]{
		subIndex: subIndex,
		indexer:  subIndex.Schema().Indexer,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// ID returns ID of the index.
func (i *UniqueIndex[T]) ID() uint64 {
	return i.id
}

// Schema returns memdb index schema.
func (i *UniqueIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  true,
		Indexer: i.indexer,
	}
}

// Type returns type of entity index is created for.
func (i *UniqueIndex[T]) Type() reflect.Type {
	return reflect.TypeFor[T]()
}

func (i *UniqueIndex[T]) dummyTDefiner(t T) {
	panic("it should never be called")
}
