package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

// NewUniqueIndex creates new unique index.
func NewUniqueIndex(subIndex memdb.Index) *UniqueIndex {
	index := &UniqueIndex{
		subIndex: subIndex,
		indexer:  subIndex.Schema().Indexer,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// UniqueIndex marks the subindex definition as unique.
type UniqueIndex struct {
	id       uint64
	subIndex memdb.Index
	indexer  memdb.Indexer
}

// ID returns ID of the index.
func (i *UniqueIndex) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *UniqueIndex) Type() reflect.Type {
	return i.subIndex.Type()
}

// Schema returns memdb index schema.
func (i *UniqueIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  true,
		Indexer: i.indexer,
	}
}
