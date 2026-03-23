package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

// FuncIndex is an index based on values returned from function.
type FuncIndex[T any] struct {
	id      uint64
	indexer *funcIndexer[T]
}

// NewFuncIndex1 creates index from 1 result.
func NewFuncIndex1[T any, V1 fieldConstraint](
	f func(ePtr *T) *V1,
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer[T]{}

	i := newFuncIndexer[T]([]reflect.Type{
		reflect.TypeFor[V1](),
	})
	i.f = func(b []byte, o *T) uint64 {
		return i.sis[0].FromObject(b, unsafe.Pointer(f(o)))
	}

	index := &FuncIndex[T]{
		indexer: i,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex2 creates index from 2 results.
func NewFuncIndex2[T any, V1, V2 fieldConstraint](
	f func(ePtr *T) (*V1, *V2),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)

	i := newFuncIndexer[T]([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
	})
	i.f = func(b []byte, o *T) uint64 {
		v1, v2 := f(o)

		n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
		n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
		return n
	}

	index := &FuncIndex[T]{
		indexer: i,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex3 creates index from 3 results.
func NewFuncIndex3[T any, V1, V2, V3 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)

	i := newFuncIndexer[T]([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
	})
	i.f = func(b []byte, o *T) uint64 {
		v1, v2, v3 := f(o)

		n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
		n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
		n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
		return n
	}

	index := &FuncIndex[T]{
		indexer: i,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex4 creates index from 4 results.
func NewFuncIndex4[T any, V1, V2, V3, V4 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3, *V4),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)

	i := newFuncIndexer[T]([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
		reflect.TypeFor[V4](),
	})
	i.f = func(b []byte, o *T) uint64 {
		v1, v2, v3, v4 := f(o)

		n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
		n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
		n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
		n += i.sis[3].FromObject(b[n:], unsafe.Pointer(v4))
		return n
	}

	index := &FuncIndex[T]{
		indexer: i,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex5 creates index from 5 results.
func NewFuncIndex5[T any, V1, V2, V3, V4, V5 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3, *V4, *V5),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)

	i := newFuncIndexer[T]([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
		reflect.TypeFor[V4](),
		reflect.TypeFor[V5](),
	})
	i.f = func(b []byte, o *T) uint64 {
		v1, v2, v3, v4, v5 := f(o)

		n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
		n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
		n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
		n += i.sis[3].FromObject(b[n:], unsafe.Pointer(v4))
		n += i.sis[4].FromObject(b[n:], unsafe.Pointer(v5))
		return n
	}

	index := &FuncIndex[T]{
		indexer: i,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex6 creates index from 6 results.
func NewFuncIndex6[T any, V1, V2, V3, V4, V5, V6 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3, *V4, *V5, *V6),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)

	i := newFuncIndexer[T]([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
		reflect.TypeFor[V4](),
		reflect.TypeFor[V5](),
		reflect.TypeFor[V6](),
	})
	i.f = func(b []byte, o *T) uint64 {
		v1, v2, v3, v4, v5, v6 := f(o)

		n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
		n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
		n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
		n += i.sis[3].FromObject(b[n:], unsafe.Pointer(v4))
		n += i.sis[4].FromObject(b[n:], unsafe.Pointer(v5))
		n += i.sis[5].FromObject(b[n:], unsafe.Pointer(v6))
		return n
	}

	index := &FuncIndex[T]{
		indexer: i,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// ID returns ID of the index.
func (i *FuncIndex[T]) ID() uint64 {
	return i.id
}

// Schema returns memdb index schema.
func (i *FuncIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Indexer: i.indexer,
	}
}

// Type returns type of entity index is created for.
func (i *FuncIndex[T]) Type() reflect.Type {
	return reflect.TypeFor[T]()
}

func (i *FuncIndex[T]) dummyTDefiner(t T) {
	panic("it should never be called")
}

type funcIndexer[T any] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(b []byte, o *T) uint64
}

func (i *funcIndexer[T]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer[T]) SizeFromObject(o unsafe.Pointer) uint64 {
	var size uint64
	for _, si := range i.sis {
		s := si.SizeFromObject(o)
		if s == 0 {
			return 0
		}
		size += s
	}
	return size
}

func (i *funcIndexer[T]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	return i.f(b, (*T)(o))
}

func newFuncIndexer[T any](types []reflect.Type) *funcIndexer[T] {
	indexer := &funcIndexer[T]{
		sis:  make([]memdb.Indexer, 0, len(types)),
		args: make([]memdb.ArgSerializer, 0, len(types)),
	}

	for _, t := range types {
		subIndexer := indexerForType(t, 0)
		indexer.sis = append(indexer.sis, subIndexer)
		indexer.args = append(indexer.args, subIndexer.Args()...)
	}

	return indexer
}
