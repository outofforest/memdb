package id

import (
	"reflect"
	"unsafe"
)

const (
	// Length is the length of ID value.
	Length = 16

	// IndexID is the ID of the ID index.
	IndexID = 0
)

// ID is used to define ID field in entities.
type ID [Length]byte

var idType = reflect.TypeOf(ID{})

// Indexer is used to index ID fields.
type Indexer struct{}

// SizeFromObject returns expected index slice size given the object.
func (i Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return Length
}

// SizeFromArgs returns expected index slice size given the arguments.
func (i Indexer) SizeFromArgs(args ...any) uint64 {
	return Length
}

// FromArgs sets index slice given the arguments.
func (i Indexer) FromArgs(b []byte, args ...any) uint64 {
	id := reflect.ValueOf(args[0]).Convert(idType).Interface().(ID)
	copy(b, id[:])
	return Length
}

// FromObject sets index slice given the object.
func (i Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	copy(b, unsafe.Slice((*byte)(o), Length))
	return Length
}
