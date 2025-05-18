package indices

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// NewMultiIndex creates new multiindex.
func NewMultiIndex(subIndices ...memdb.Index) *MultiIndex {
	if len(subIndices) == 0 {
		panic(errors.Errorf("no subindices has been provided"))
	}

	t := subIndices[0].Type()

	var numOfArgs uint64
	subIndexers := make([]memdb.Indexer, 0, len(subIndices))
	for _, si := range subIndices {
		if si.Type() != t {
			panic(errors.Errorf("wrong type, expected: %s, got: %s", t, si.Type()))
		}
		numOfArgs += si.NumOfArgs()
		schema := si.Schema()
		subIndexers = append(subIndexers, schema.Indexer)
	}

	index := &MultiIndex{
		numOfArgs:  numOfArgs,
		entityType: t,
		indexer: &multiIndexer{
			subIndices:  subIndices,
			subIndexers: subIndexers,
		},
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// MultiIndex compiles many indices into a single one.
type MultiIndex struct {
	id         uint64
	numOfArgs  uint64
	entityType reflect.Type
	indexer    memdb.Indexer
}

// ID returns ID of the index.
func (i *MultiIndex) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *MultiIndex) Type() reflect.Type {
	return i.entityType
}

// NumOfArgs returns number of arguments taken by the index.
func (i *MultiIndex) NumOfArgs() uint64 {
	return i.numOfArgs
}

// Schema returns memdb index schema.
func (i *MultiIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Indexer: i.indexer,
	}
}

var _ memdb.Indexer = &multiIndexer{}

type multiIndexer struct {
	subIndices  []memdb.Index
	subIndexers []memdb.Indexer
}

func (mi *multiIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
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

func (mi *multiIndexer) SizeFromArgs(args ...any) uint64 {
	var startArg uint64
	var n uint64

	for i, index := range mi.subIndices {
		if startArg >= uint64(len(args)) {
			break
		}
		numOfArgs := index.NumOfArgs()
		if startArg+numOfArgs > uint64(len(args)) {
			n += mi.subIndexers[i].SizeFromArgs(args[startArg:]...)
		} else {
			n += mi.subIndexers[i].SizeFromArgs(args[startArg : startArg+numOfArgs]...)
		}
		startArg += numOfArgs
	}
	return n
}

func (mi *multiIndexer) FromArgs(b []byte, args ...any) uint64 {
	var startArg uint64
	var n uint64

	for i, index := range mi.subIndices {
		if startArg >= uint64(len(args)) {
			break
		}
		numOfArgs := index.NumOfArgs()
		if startArg+numOfArgs > uint64(len(args)) {
			n += mi.subIndexers[i].FromArgs(b[n:], args[startArg:]...)
		} else {
			n += mi.subIndexers[i].FromArgs(b[n:], args[startArg:startArg+numOfArgs]...)
		}
		startArg += numOfArgs
	}
	return n
}

func (mi *multiIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	var n uint64
	for _, si := range mi.subIndexers {
		n += si.FromObject(b[n:], o)
	}
	return n
}
