package memdb

import (
	"crypto/rand"
	"reflect"
	"unsafe"

	"github.com/samber/lo"
)

const (
	// IDLength is the length of ID value.
	IDLength = 16

	// IDIndexID is the ID of the ID index.
	IDIndexID = 0
)

// ID is used to define ID field in entities.
type ID [IDLength]byte

type idConstraint interface {
	~[IDLength]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	var id [IDLength]byte
	lo.Must(rand.Read(id[:]))
	return id
}

var idType = reflect.TypeOf(ID{})

var _ Indexer = IDIndexer{}
var _ ArgSerializer = IDIndexer{}

// IDIndexer is used to index ID fields.
type IDIndexer struct{}

// Args returns arg serializer.
func (i IDIndexer) Args() []ArgSerializer {
	return []ArgSerializer{i}
}

// SizeFromObject returns expected index slice size given the object.
func (i IDIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return IDLength
}

// SizeFromArg returns expected index slice size given the argument.
func (i IDIndexer) SizeFromArg(arg any) uint64 {
	return IDLength
}

// FromArg sets index slice given the argument.
func (i IDIndexer) FromArg(b []byte, arg any) uint64 {
	id := reflect.ValueOf(arg).Convert(idType).Interface().(ID)
	copy(b, id[:])
	return IDLength
}

// FromObject sets index slice given the object.
func (i IDIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	copy(b, unsafe.Slice((*byte)(o), IDLength))
	return IDLength
}
