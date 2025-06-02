package indices

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	xyz = "XYZ"
	ijk = "IJK"
)

func TestMultiIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)

	index := NewMultiIndex(index1, index2)
	requireT.NotZero(index.ID())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.False(index.Schema().Unique)

	indexer := index.Schema().Indexer.(*multiIndexer)
	requireT.Len(indexer.Args(), 2)

	v.Value1 = 5
	v.Value4 = xyz
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x58, 0x59, 0x5a, 0x0}, v)
}

func TestMultiIndexerNotAllArguments(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)

	index := NewMultiIndex(index1, index2)

	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5}, verifyPart{o: v})
}

func TestMultiIndexerWithMultiSubIndexer3Arguments(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)
	index3 := NewFieldIndex(v, &v.Value2.Value3)
	index4 := NewMultiIndex(index1, index2)

	index := NewMultiIndex(index3, index4)
	requireT.NotZero(index.ID())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(*multiIndexer)
	requireT.Len(indexer.Args(), 3)

	v.Value1 = 5
	v.Value4 = xyz
	v.Value2.Value3 = ijk
	verifyObject(requireT, indexer,
		[]byte{0x49, 0x4a, 0x4b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x58, 0x59, 0x5a, 0x0}, v)
}

func TestMultiIndexerWithMultiSubIndexer2Arguments(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)
	index3 := NewFieldIndex(v, &v.Value2.Value3)
	index4 := NewMultiIndex(index1, index2)

	index := NewMultiIndex(index3, index4)
	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	v.Value2.Value3 = ijk
	verifyObject(requireT, indexer, []byte{0x49, 0x4a, 0x4b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5},
		verifyPart{o: v})
}

func TestMultiIndexerWithMultiSubIndexer1Argument(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)
	index3 := NewFieldIndex(v, &v.Value2.Value3)
	index4 := NewMultiIndex(index1, index2)

	index := NewMultiIndex(index3, index4)

	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	v.Value2.Value3 = ijk
	verifyObject(requireT, indexer, []byte{0x49, 0x4a, 0x4b, 0x0}, verifyPart{o: v})
}

func TestMultiIndexerWithIfSubindex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)
	index3 := NewIfIndex(index2, ifFunc[o](o{Value1: 1, Value4: abc}, o{Value1: 2, Value4: xyz}))

	index := NewMultiIndex(index1, index3)
	requireT.NotZero(index.ID())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(*multiIndexer)
	requireT.Len(indexer.Args(), 2)

	v.Value1 = 1
	v.Value4 = abc
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0}, v)

	v.Value1 = 2
	v.Value4 = xyz
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x58, 0x59, 0x5a, 0x0}, v)

	v.Value1 = 1
	v.Value4 = xyz
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x58, 0x59, 0x5a, 0x0},
		verifyMissing{o: v})
}

func TestMultiIndexerWithUniqueSubindex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index1 := NewFieldIndex(v, &v.Value1)
	index2 := NewFieldIndex(v, &v.Value4)
	index3 := NewUniqueIndex(index2)

	index := NewMultiIndex(index1, index3)
	requireT.NotZero(index.ID())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.True(index.Schema().Unique)

	indexer := index.Schema().Indexer.(*multiIndexer)
	requireT.Len(indexer.Args(), 2)

	v.Value1 = 1
	v.Value4 = abc
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0}, v)

	v.Value1 = 2
	v.Value4 = xyz
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x58, 0x59, 0x5a, 0x0}, v)
}

func TestMultiErrorIfNoSubIndices(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	requireT.Panics(func() {
		NewMultiIndex()
	})
}

func TestMultiErrorOnTypeMismatch(t *testing.T) {
	requireT := require.New(t)
	v1 := &o{}
	v2 := &subO1{}

	index1 := NewFieldIndex(v1, &v1.Value1)
	index2 := NewFieldIndex(v2, &v2.Value3)

	requireT.Panics(func() {
		NewMultiIndex(index1, index2)
	})
}
