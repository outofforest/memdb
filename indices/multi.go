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

	var unique bool
	var args []memdb.ArgSerializer
	subIndexers := make([]memdb.Indexer, 0, len(subIndices))
	for _, si := range subIndices {
		if si.Type() != t {
			panic(errors.Errorf("wrong type, expected: %s, got: %s", t, si.Type()))
		}
		schema := si.Schema()
		subIndexers = append(subIndexers, schema.Indexer)
		unique = unique || schema.Unique
		args = append(args, schema.Indexer.Args()...)
	}

	index := &MultiIndex{
		entityType: t,
		indexer: &multiIndexer{
			subIndices:  subIndices,
			subIndexers: subIndexers,
			args:        args,
		},
		unique: unique,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// MultiIndex compiles many indices into a single one.
type MultiIndex struct {
	id         uint64
	entityType reflect.Type
	indexer    memdb.Indexer
	unique     bool
}

// ID returns ID of the index.
func (i *MultiIndex) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *MultiIndex) Type() reflect.Type {
	return i.entityType
}

// Schema returns memdb index schema.
func (i *MultiIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Unique:  i.unique,
		Indexer: i.indexer,
	}
}

var _ memdb.Indexer = &multiIndexer{}

type multiIndexer struct {
	subIndices  []memdb.Index
	subIndexers []memdb.Indexer
	args        []memdb.ArgSerializer
}

func (mi *multiIndexer) Args() []memdb.ArgSerializer {
	return mi.args
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

func (mi *multiIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	var n uint64
	for _, si := range mi.subIndexers {
		n += si.FromObject(b[n:], o)
	}
	return n
}
