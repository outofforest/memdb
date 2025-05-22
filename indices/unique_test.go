package indices

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniqueIndex(t *testing.T) {
	requireT := require.New(t)

	var v o
	subIndex := NewMultiIndex(NewFieldIndex(&v, &v.Value1), NewFieldIndex(&v, &v.Value4))
	index := NewUniqueIndex(subIndex)

	requireT.True(index.Schema().Unique)
	requireT.Equal(subIndex.Schema().Indexer, index.Schema().Indexer)
	requireT.Equal(subIndex.Type(), index.Type())
	requireT.Equal(subIndex.NumOfArgs(), index.NumOfArgs())
	requireT.NotEqual(subIndex.ID(), index.ID())
}
