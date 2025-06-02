package memdb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type entity struct {
	ID ID
}

func TestEntityIDIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &entity{}

	indexer := IDIndexer{}

	v.ID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	verify(requireT, indexer, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, v, v.ID)
}

func verify(
	requireT *require.Assertions,
	indexer ArgSerializerIndexer,
	expected []byte,
	o any, arg any,
) {
	size := indexer.SizeFromArg(arg)
	requireT.EqualValues(len(expected), size)
	b := make([]byte, size)
	requireT.Equal(size, indexer.FromArg(b, arg))
	requireT.Equal(expected, b)

	size2 := indexer.SizeFromObject(reflect.ValueOf(o).UnsafePointer())
	requireT.Equal(size, size2)
	b = make([]byte, size)
	requireT.Equal(size, indexer.FromObject(b, reflect.ValueOf(o).UnsafePointer()))
	requireT.Equal(expected, b)
}
