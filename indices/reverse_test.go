package indices

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReverseIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewFieldIndex(v, &v.Value1)

	index := NewReverseIndex(subIndex)
	requireT.NotZero(index.ID())
	requireT.Equal(subIndex.Type(), index.Type())
	requireT.NotEqual(subIndex.ID(), index.ID())
	requireT.False(index.Schema().Unique)
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(reverseIndexer)
	requireT.Len(indexer.Args(), 1)

	v.Value1 = 1
	verify(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, v, v.Value1)

	v.Value1 = 2
	verify(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd}, v, v.Value1)

	v.Value1 = 3
	verify(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc}, v, v.Value1)
}

func TestReverseIndexerUnique(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewUniqueIndex(NewFieldIndex(v, &v.Value1))

	index := NewReverseIndex(subIndex)
	requireT.NotZero(index.ID())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.True(index.Schema().Unique)

	indexer := index.Schema().Indexer.(reverseIndexer)
	requireT.Len(indexer.Args(), 1)

	v.Value1 = 1
	verifyObject(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, v)

	v.Value1 = 1
	verifyObject(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, v)

	v.Value1 = 2
	verifyObject(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd}, v)
}

func TestNegate(t *testing.T) {
	t.Parallel()

	for n := range 1000 {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			requireT := require.New(t)

			t.Parallel()

			b := make([]byte, n)
			_, err := rand.Read(b)
			requireT.NoError(err)

			e := make([]byte, n)
			for i, v := range b {
				e[i] = v ^ 0xFF
			}

			negate(b)

			requireT.Equal(e, b)
		})
	}
}
