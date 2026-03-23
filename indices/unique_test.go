package indices

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniqueIndexType(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	subIndex := NewMultiIndex(NewFieldIndex(&v, &v.Value1), NewFieldIndex(&v, &v.Value4))
	index := NewUniqueIndex(subIndex)

	requireT.Equal(reflect.TypeFor[o](), index.Type())
}

func TestUniqueIndex(t *testing.T) {
	requireT := require.New(t)

	var v o
	subIndex := NewMultiIndex(NewFieldIndex(&v, &v.Value1), NewFieldIndex(&v, &v.Value4))
	index := NewUniqueIndex(subIndex)

	requireT.True(index.Schema().Unique)
	requireT.Equal(subIndex.Schema().Indexer, index.Schema().Indexer)
	requireT.Equal(subIndex.Schema().Indexer.Args(), index.Schema().Indexer.Args())
	requireT.NotEqual(subIndex.ID(), index.ID())
}
