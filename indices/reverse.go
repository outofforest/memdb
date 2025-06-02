package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

// NewReverseIndex creates new order reversing index.
func NewReverseIndex(subIndex memdb.Index) *ReverseIndex {
	schema := subIndex.Schema()
	index := &ReverseIndex{
		subIndex: subIndex,
		indexer: reverseIndexer{
			subIndexer: schema.Indexer.(memdb.ArgSerializerIndexer),
		},
		unique: schema.Unique,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// ReverseIndex reverses the order of elements in the index by reversing all the bits of the index key.
type ReverseIndex struct {
	id       uint64
	subIndex memdb.Index
	indexer  memdb.Indexer
	unique   bool
}

// ID returns ID of the index.
func (i *ReverseIndex) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *ReverseIndex) Type() reflect.Type {
	return i.subIndex.Type()
}

// Schema returns memdb index schema.
func (i *ReverseIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  i.unique,
		Indexer: i.indexer,
	}
}

var _ memdb.Indexer = reverseIndexer{}
var _ memdb.ArgSerializer = reverseIndexer{}

type reverseIndexer struct {
	subIndexer memdb.ArgSerializerIndexer
}

func (i reverseIndexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i reverseIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return i.subIndexer.SizeFromObject(o)
}

func (i reverseIndexer) SizeFromArg(arg any) uint64 {
	return i.subIndexer.SizeFromArg(arg)
}

func (i reverseIndexer) FromArg(b []byte, arg any) uint64 {
	n := i.subIndexer.FromArg(b, arg)
	negate(b[:n])
	return n
}

func (i reverseIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	n := i.subIndexer.FromObject(b, o)
	negate(b[:n])
	return n
}

func negate(b []byte) {
	if len(b) == 0 {
		return
	}

	l := len(b)
	p := unsafe.Pointer(&b[0])
	for ; l >= 8; l, p = l-8, unsafe.Add(p, 8) {
		*(*uint64)(p) ^= 0xFFFFFFFFFFFFFFFF
	}

	if l >= 4 {
		switch l {
		case 7:
			*(*uint32)(p) ^= 0xFFFFFFFF
			*(*uint16)(unsafe.Add(p, 4)) ^= 0xFFFF
			*(*uint8)(unsafe.Add(p, 6)) ^= 0xFF
		case 6:
			*(*uint32)(p) ^= 0xFFFFFFFF
			*(*uint16)(unsafe.Add(p, 4)) ^= 0xFFFF
		case 5:
			*(*uint32)(p) ^= 0xFFFFFFFF
			*(*uint8)(unsafe.Add(p, 4)) ^= 0xFF
		default:
			*(*uint32)(p) ^= 0xFFFFFFFF
		}
	} else {
		switch l {
		case 3:
			*(*uint16)(p) ^= 0xFFFF
			*(*uint8)(unsafe.Add(p, 2)) ^= 0xFF
		case 2:
			*(*uint16)(p) ^= 0xFFFF
		case 1:
			*(*uint8)(p) ^= 0xFF
		}
	}
}
