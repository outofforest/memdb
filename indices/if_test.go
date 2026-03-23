//nolint:testifylint
package indices

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
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

func TestIfIndexType(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	subIndex := NewFieldIndex(&v, &v.Value1)
	index := NewIfIndex(subIndex, ifFunc[o](o{Value1: 1}, o{Value1: 2}))

	requireT.Equal(reflect.TypeFor[o](), index.Type())
}

func TestIfIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewFieldIndex(v, &v.Value1)

	index := NewIfIndex(subIndex, ifFunc[o](o{Value1: 1}, o{Value1: 2}))
	requireT.NotZero(index.ID())
	requireT.NotEqual(subIndex.ID(), index.ID())
	requireT.False(index.Schema().Unique)

	indexer := index.Schema().Indexer.(ifIndexer[o])
	requireT.Len(indexer.Args(), 1)

	v.Value1 = 1
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}, v)

	v.Value1 = 2
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2}, v)

	v.Value1 = 3
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3}, verifyMissing{o: v})
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

	indexer := index.Schema().Indexer.(ifIndexer[o])
	requireT.Len(indexer.Args(), 2)

	v.Value1 = 1
	v.Value4 = abc
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0}, v)

	v.Value1 = 1
	v.Value4 = def
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x44, 0x45, 0x46, 0x0}, v)

	v.Value1 = 2
	v.Value4 = abc
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x41, 0x42, 0x43, 0x0},
		verifyMissing{o: v})
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
	requireT.True(index.Schema().Unique)

	indexer := index.Schema().Indexer.(ifIndexer[o])
	requireT.Len(indexer.Args(), 2)

	v.Value1 = 1
	v.Value4 = abc
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0}, v)

	v.Value1 = 1
	v.Value4 = def
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x44, 0x45, 0x46, 0x0}, v)

	v.Value1 = 2
	v.Value4 = abc
	verifyObject(requireT, indexer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x41, 0x42, 0x43, 0x0},
		verifyMissing{o: v})
}

func TestEntityUpdateWithIfIndex(t *testing.T) {
	requireT := require.New(t)

	var v o
	index := NewIfIndex(NewFieldIndex(&v, &v.Value1), func(v *o) bool {
		return v.Value1 == 1
	})

	c := memdb.Config{
		Entities: []reflect.Type{reflect.TypeFor[o]()},
		Indices:  []memdb.Index{index},
	}

	db, err := memdb.NewMemDB(c)
	requireT.NoError(err)
	txn := db.Txn(true)

	eID := memdb.NewID[memdb.ID]()
	e := &o{
		ID:     eID,
		Value1: 1,
	}

	old, err := txn.Insert(0, unsafe.Pointer(e))
	requireT.NoError(err)
	requireT.Zero(old)
	txn.Commit()

	txn = db.Txn(true)
	e2, err := txn.First(0, memdb.IDIndexID, eID)
	requireT.NoError(err)
	requireT.NotZero(e2)
	requireT.Equal(e, (*o)(e2))

	e3, err := txn.First(0, index.ID(), uint64(1))
	requireT.NoError(err)
	requireT.NotZero(e3)
	requireT.Equal(e2, e3)

	e4 := &o{
		ID:     eID,
		Value1: 2,
	}

	old, err = txn.Insert(0, unsafe.Pointer(e4))
	requireT.NoError(err)
	requireT.NotZero(old)
	requireT.Equal(e, (*o)(old))
	txn.Commit()

	txn = db.Txn(false)
	e2, err = txn.First(0, memdb.IDIndexID, eID)
	requireT.NoError(err)
	requireT.NotZero(e2)
	requireT.Equal(e4, (*o)(e2))

	e3, err = txn.First(0, index.ID(), uint64(2))
	requireT.NoError(err)
	requireT.Zero(e3)

	e3, err = txn.First(0, index.ID(), uint64(1))
	requireT.NoError(err)
	requireT.Zero(e3)
}
