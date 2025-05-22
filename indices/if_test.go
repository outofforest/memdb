package indices

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func ifFunc[T comparable](values ...T) func(o *T) bool {
	vs := map[T]bool{}
	for _, v := range values {
		vs[v] = true
	}

	return func(o *T) bool {
		return vs[*o]
	}
}

func TestIfIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewFieldIndex(v, &v.Value1)

	index := NewIfIndex(subIndex, ifFunc[o](o{Value1: 1}, o{Value1: 2}))
	requireT.NotZero(index.ID())
	requireT.Equal(subIndex.Type(), index.Type())
	requireT.Equal(subIndex.NumOfArgs(), index.NumOfArgs())
	requireT.NotEqual(subIndex.ID(), index.ID())
	requireT.False(index.Schema().Unique)
	requireT.EqualValues(1, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(ifIndexer[o])

	v.Value1 = 1
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}, v, v.Value1)

	v.Value1 = 2
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2}, v, v.Value1)

	v.Value1 = 3
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3}, verifyMissing{o: v}, v.Value1)
}

func TestIfIndexerMulti(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)

	subIndex := NewMultiIndex(index1, index2)

	index := NewIfIndex(subIndex, ifFunc[o](o{Value1: 1, Value4: abc}, o{Value1: 1, Value4: def}))
	requireT.NotZero(index.ID())
	requireT.EqualValues(2, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(ifIndexer[o])

	v.Value1 = 1
	v.Value4 = abc
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0},
		v, v.Value1, v.Value4)

	v.Value1 = 1
	v.Value4 = def
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x44, 0x45, 0x46, 0x0},
		v, v.Value1, v.Value4)

	v.Value1 = 2
	v.Value4 = abc
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x41, 0x42, 0x43, 0x0},
		verifyMissing{o: v}, v.Value1, v.Value4)
}

func TestIfIndexerUnique(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)

	subIndex := NewUniqueIndex(NewMultiIndex(index1, index2))

	index := NewIfIndex(subIndex, ifFunc[o](o{Value1: 1, Value4: abc}, o{Value1: 1, Value4: def}))
	requireT.NotZero(index.ID())
	requireT.EqualValues(2, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.True(index.Schema().Unique)

	indexer := index.Schema().Indexer.(ifIndexer[o])

	v.Value1 = 1
	v.Value4 = abc
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0},
		v, v.Value1, v.Value4)

	v.Value1 = 1
	v.Value4 = def
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x44, 0x45, 0x46, 0x0},
		v, v.Value1, v.Value4)

	v.Value1 = 2
	v.Value4 = abc
	verify(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x41, 0x42, 0x43, 0x0},
		verifyMissing{o: v}, v.Value1, v.Value4)
}

func TestIfIndexerErrorOnTypeMismatch(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewFieldIndex(v, &v.Value1)

	requireT.Panics(func() {
		NewIfIndex(subIndex, ifFunc[subO1](subO1{Value1: 1}, subO1{Value1: 2}))
	})
}
