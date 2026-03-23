package indices

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/memdb"
)

// FuncIndex is an index based on values returned from function.
type FuncIndex[T any] struct {
	id      uint64
	indexer memdb.Indexer
}

// NewFuncIndex1 creates index from 1 result.
func NewFuncIndex1[T any, V1 fieldConstraint](
	f func(ePtr *T) *V1,
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer1[T, V1]{}

	sis, args := funcIndexArgs([]reflect.Type{
		reflect.TypeFor[V1](),
	})
	index := &FuncIndex[T]{
		indexer: &funcIndexer1[T, V1]{
			sis:  sis,
			args: args,
			f:    f,
		},
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex2 creates index from 2 results.
func NewFuncIndex2[T any, V1, V2 fieldConstraint](
	f func(ePtr *T) (*V1, *V2),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer2[T, V1, V2]{}

	sis, args := funcIndexArgs([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
	})
	index := &FuncIndex[T]{
		indexer: &funcIndexer2[T, V1, V2]{
			sis:  sis,
			args: args,
			f:    f,
		},
	}

	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex3 creates index from 3 results.
func NewFuncIndex3[T any, V1, V2, V3 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer3[T, V1, V2, V3]{}

	sis, args := funcIndexArgs([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
	})
	index := &FuncIndex[T]{
		indexer: &funcIndexer3[T, V1, V2, V3]{
			sis:  sis,
			args: args,
			f:    f,
		},
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex4 creates index from 4 results.
func NewFuncIndex4[T any, V1, V2, V3, V4 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3, *V4),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer4[T, V1, V2, V3, V4]{}

	sis, args := funcIndexArgs([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
		reflect.TypeFor[V4](),
	})
	index := &FuncIndex[T]{
		indexer: &funcIndexer4[T, V1, V2, V3, V4]{
			sis:  sis,
			args: args,
			f:    f,
		},
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex5 creates index from 5 results.
func NewFuncIndex5[T any, V1, V2, V3, V4, V5 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3, *V4, *V5),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer5[T, V1, V2, V3, V4, V5]{}

	sis, args := funcIndexArgs([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
		reflect.TypeFor[V4](),
		reflect.TypeFor[V5](),
	})
	index := &FuncIndex[T]{
		indexer: &funcIndexer5[T, V1, V2, V3, V4, V5]{
			sis:  sis,
			args: args,
			f:    f,
		},
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// NewFuncIndex6 creates index from 6 results.
func NewFuncIndex6[T any, V1, V2, V3, V4, V5, V6 fieldConstraint](
	f func(ePtr *T) (*V1, *V2, *V3, *V4, *V5, *V6),
) *FuncIndex[T] {
	var _ Index[T] = (*FuncIndex[T])(nil)
	var _ memdb.Indexer = &funcIndexer6[T, V1, V2, V3, V4, V5, V6]{}

	sis, args := funcIndexArgs([]reflect.Type{
		reflect.TypeFor[V1](),
		reflect.TypeFor[V2](),
		reflect.TypeFor[V3](),
		reflect.TypeFor[V4](),
		reflect.TypeFor[V5](),
		reflect.TypeFor[V6](),
	})
	index := &FuncIndex[T]{
		indexer: &funcIndexer6[T, V1, V2, V3, V4, V5, V6]{
			sis:  sis,
			args: args,
			f:    f,
		},
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

type funcIndexer1[T any, V1 fieldConstraint] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(ePtr *T) *V1
}

func (i *funcIndexer1[T, V1]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer1[T, V1]) SizeFromObject(o unsafe.Pointer) uint64 {
	return i.sis[0].SizeFromObject(unsafe.Pointer(i.f((*T)(o))))
}

func (i *funcIndexer1[T, V1]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	return i.sis[0].FromObject(b, unsafe.Pointer(i.f((*T)(o))))
}

type funcIndexer2[T any, V1, V2 fieldConstraint] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(ePtr *T) (*V1, *V2)
}

func (i *funcIndexer2[T, V1, V2]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer2[T, V1, V2]) SizeFromObject(o unsafe.Pointer) uint64 {
	v1, v2 := i.f((*T)(o))
	return i.sis[0].SizeFromObject(unsafe.Pointer(v1)) +
		i.sis[1].SizeFromObject(unsafe.Pointer(v2))
}

func (i *funcIndexer2[T, V1, V2]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	v1, v2 := i.f((*T)(o))

	n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
	n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
	return n
}

type funcIndexer3[T any, V1, V2, V3 fieldConstraint] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(ePtr *T) (*V1, *V2, *V3)
}

func (i *funcIndexer3[T, V1, V2, V3]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer3[T, V1, V2, V3]) SizeFromObject(o unsafe.Pointer) uint64 {
	v1, v2, v3 := i.f((*T)(o))
	return i.sis[0].SizeFromObject(unsafe.Pointer(v1)) +
		i.sis[1].SizeFromObject(unsafe.Pointer(v2)) +
		i.sis[2].SizeFromObject(unsafe.Pointer(v3))
}

func (i *funcIndexer3[T, V1, V2, V3]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	v1, v2, v3 := i.f((*T)(o))

	n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
	n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
	n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
	return n
}

type funcIndexer4[T any, V1, V2, V3, V4 fieldConstraint] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(ePtr *T) (*V1, *V2, *V3, *V4)
}

func (i *funcIndexer4[T, V1, V2, V3, V4]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer4[T, V1, V2, V3, V4]) SizeFromObject(o unsafe.Pointer) uint64 {
	v1, v2, v3, v4 := i.f((*T)(o))
	return i.sis[0].SizeFromObject(unsafe.Pointer(v1)) +
		i.sis[1].SizeFromObject(unsafe.Pointer(v2)) +
		i.sis[2].SizeFromObject(unsafe.Pointer(v3)) +
		i.sis[3].SizeFromObject(unsafe.Pointer(v4))
}

func (i *funcIndexer4[T, V1, V2, V3, V4]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	v1, v2, v3, v4 := i.f((*T)(o))

	n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
	n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
	n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
	n += i.sis[3].FromObject(b[n:], unsafe.Pointer(v4))
	return n
}

type funcIndexer5[T any, V1, V2, V3, V4, V5 fieldConstraint] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(ePtr *T) (*V1, *V2, *V3, *V4, *V5)
}

func (i *funcIndexer5[T, V1, V2, V3, V4, V5]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer5[T, V1, V2, V3, V4, V5]) SizeFromObject(o unsafe.Pointer) uint64 {
	v1, v2, v3, v4, v5 := i.f((*T)(o))
	return i.sis[0].SizeFromObject(unsafe.Pointer(v1)) +
		i.sis[1].SizeFromObject(unsafe.Pointer(v2)) +
		i.sis[2].SizeFromObject(unsafe.Pointer(v3)) +
		i.sis[3].SizeFromObject(unsafe.Pointer(v4)) +
		i.sis[4].SizeFromObject(unsafe.Pointer(v5))
}

func (i *funcIndexer5[T, V1, V2, V3, V4, V5]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	v1, v2, v3, v4, v5 := i.f((*T)(o))

	n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
	n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
	n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
	n += i.sis[3].FromObject(b[n:], unsafe.Pointer(v4))
	n += i.sis[4].FromObject(b[n:], unsafe.Pointer(v5))
	return n
}

type funcIndexer6[T any, V1, V2, V3, V4, V5, V6 fieldConstraint] struct {
	sis  []memdb.Indexer
	args []memdb.ArgSerializer
	f    func(ePtr *T) (*V1, *V2, *V3, *V4, *V5, *V6)
}

func (i *funcIndexer6[T, V1, V2, V3, V4, V5, V6]) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *funcIndexer6[T, V1, V2, V3, V4, V5, V6]) SizeFromObject(o unsafe.Pointer) uint64 {
	v1, v2, v3, v4, v5, v6 := i.f((*T)(o))
	return i.sis[0].SizeFromObject(unsafe.Pointer(v1)) +
		i.sis[1].SizeFromObject(unsafe.Pointer(v2)) +
		i.sis[2].SizeFromObject(unsafe.Pointer(v3)) +
		i.sis[3].SizeFromObject(unsafe.Pointer(v4)) +
		i.sis[4].SizeFromObject(unsafe.Pointer(v5)) +
		i.sis[5].SizeFromObject(unsafe.Pointer(v6))
}

func (i *funcIndexer6[T, V1, V2, V3, V4, V5, V6]) FromObject(b []byte, o unsafe.Pointer) uint64 {
	v1, v2, v3, v4, v5, v6 := i.f((*T)(o))

	n := i.sis[0].FromObject(b, unsafe.Pointer(v1))
	n += i.sis[1].FromObject(b[n:], unsafe.Pointer(v2))
	n += i.sis[2].FromObject(b[n:], unsafe.Pointer(v3))
	n += i.sis[3].FromObject(b[n:], unsafe.Pointer(v4))
	n += i.sis[4].FromObject(b[n:], unsafe.Pointer(v5))
	n += i.sis[5].FromObject(b[n:], unsafe.Pointer(v6))
	return n
}

func funcIndexArgs(types []reflect.Type) ([]memdb.Indexer, []memdb.ArgSerializer) {
	sis := make([]memdb.Indexer, 0, len(types))
	args := make([]memdb.ArgSerializer, 0, len(types))

	for _, t := range types {
		subIndexer := indexerForType(t, 0)
		sis = append(sis, subIndexer)
		args = append(args, subIndexer.Args()...)
	}

	return sis, args
}
