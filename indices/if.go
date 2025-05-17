package indices

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/go-memdb"
)

// NewIfIndex creates new conditional index.
func NewIfIndex[T any](name string, subIndex Index, f func(o *T) bool) *IfIndex[T] {
	var v T
	if t := reflect.TypeOf(v); t != subIndex.Type() {
		panic(errors.Errorf("subindex type mismatch, expected: %s, got: %s", t, subIndex.Type()))
	}

	return &IfIndex[T]{
		name:     subIndex.Name() + "," + name,
		subIndex: subIndex,
		indexer: ifIndexer[T]{
			subIndexer: subIndex.Schema().Indexer,
			f:          f,
		},
	}
}

var _ memdb.Indexer = ifIndexer[int]{}

// IfIndex indexes those elements from another index for which f returns true.
type IfIndex[T any] struct {
	name     string
	subIndex Index
	indexer  memdb.Indexer
}

// Name returns name of the index.
func (i *IfIndex[T]) Name() string {
	return i.name
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
		Name:    i.name,
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
