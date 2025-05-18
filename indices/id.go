package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

var idType = reflect.TypeOf(memdb.ID{})

var _ memdb.Indexer = IDIndexer{}

// IDIndexer is used to index ID fields.
type IDIndexer struct{}

// SizeFromObject returns expected index slice size given the object.
func (idi IDIndexer) SizeFromObject(o any) uint64 {
	return memdb.IDLength
}

// SizeFromArgs returns expected index slice size given the arguments.
func (idi IDIndexer) SizeFromArgs(args ...any) uint64 {
	return memdb.IDLength
}

// FromArgs sets index slice given the arguments.
func (idi IDIndexer) FromArgs(b []byte, args ...any) uint64 {
	id := reflect.ValueOf(args[0]).Convert(idType).Interface().(memdb.ID)
	copy(b, id[:])
	return memdb.IDLength
}

// FromObject sets index slice given the object.
func (idi IDIndexer) FromObject(b []byte, o any) uint64 {
	copy(b, unsafeIDFromEntity(o.(reflect.Value)))
	return memdb.IDLength
}

func unsafeIDFromEntity(eValue reflect.Value) []byte {
	return unsafe.Slice((*byte)(eValue.UnsafePointer()), memdb.IDLength)
}
