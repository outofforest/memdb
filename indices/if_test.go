package indices

import (
	"reflect"
	"testing"

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

func TestIfIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewFieldIndex(v, &v.Value1)

	index := NewIfIndex(subIndex, ifFunc[o](o{Value1: 1}, o{Value1: 2}))
	requireT.NotZero(index.ID())
	requireT.Equal(subIndex.Type(), index.Type())
	requireT.NotEqual(subIndex.ID(), index.ID())
	requireT.False(index.Schema().Unique)
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

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
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

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
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
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

func TestIfIndexerErrorOnTypeMismatch(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	subIndex := NewFieldIndex(v, &v.Value1)

	requireT.Panics(func() {
		NewIfIndex(subIndex, ifFunc[subO1](subO1{Value1: 1}, subO1{Value1: 2}))
	})
}

func TestEntityUpdateWithIfIndex(t *testing.T) {
	requireT := require.New(t)

	var v o
	index := NewIfIndex(NewFieldIndex(&v, &v.Value1), func(v *o) bool {
		return v.Value1 == 1
	})

	db, err := memdb.NewMemDB([][]memdb.Index{{index}})
	requireT.NoError(err)
	txn := db.Txn(true)

	eID := memdb.NewID[memdb.ID]()
	e := reflect.ValueOf(&o{
		ID:     eID,
		Value1: 1,
	})

	old, err := txn.Insert(0, &e)
	requireT.NoError(err)
	requireT.Nil(old)
	txn.Commit()

	txn = db.Txn(true)
	e2, err := txn.First(0, memdb.IDIndexID, eID)
	requireT.NoError(err)
	requireT.NotNil(e2)
	requireT.Equal(e.Elem().Interface(), e2.Elem().Interface())

	e3, err := txn.First(0, index.ID(), uint64(1))
	requireT.NoError(err)
	requireT.NotNil(e3)
	requireT.Equal(e2, e3)

	e4 := reflect.ValueOf(&o{
		ID:     eID,
		Value1: 2,
	})

	old, err = txn.Insert(0, &e4)
	requireT.NoError(err)
	requireT.Equal(&e, old)
	txn.Commit()

	txn = db.Txn(false)
	e2, err = txn.First(0, memdb.IDIndexID, eID)
	requireT.NoError(err)
	requireT.NotNil(e2)
	requireT.Equal(e4.Elem().Interface(), e2.Elem().Interface())

	e3, err = txn.First(0, index.ID(), uint64(2))
	requireT.NoError(err)
	requireT.Nil(e3)

	e3, err = txn.First(0, index.ID(), uint64(1))
	requireT.NoError(err)
	requireT.Nil(e3)
}
