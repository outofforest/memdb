package indices

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb/id"
)

const (
	abc = "ABC"
	def = "DEF"
)

type o struct {
	Value1 uint64
	Value2 subO1
	Value3 subO2
	Value4 string
}

type subO1 struct {
	Value1 uint64
	Value2 subO2
	Value3 string
}

type subO2 struct {
	ValueBool   bool
	ValueString string
	ValueTime   time.Time
	ValueInt8   int8
	ValueInt16  int16
	ValueInt32  int32
	ValueInt64  int64
	ValueUint8  uint8
	ValueUint16 uint16
	ValueUint32 uint32
	ValueUint64 uint64
	ValueID     id.ID
	Value1      string
	Value2      int16
	Value3      uint8
}

func TestFieldIndexOffset(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{
		Value1: 1,
		Value2: subO1{
			Value1: 2,
			Value2: subO2{
				Value1: abc,
				Value2: 2,
				Value3: 3,
			},
			Value3: def,
		},
		Value3: subO2{
			Value1: "GHI",
			Value2: 5,
			Value3: 6,
		},
		Value4: "JKM",
	}

	var v2 o

	requireT.Panics(func() {
		NewFieldIndex(v, &v2)
	})

	v3 := &v2
	requireT.Panics(func() {
		NewFieldIndex(v, &v3)
	})

	requireT.Panics(func() {
		NewFieldIndex(v, &v)
	})

	f := &v.Value1
	requireT.Panics(func() {
		NewFieldIndex(v, &f)
	})

	i := NewFieldIndex(v, &v.Value1)
	requireT.NotZero(i.ID())
	requireT.EqualValues(1, i.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), i.Type())
	requireT.Equal(uint64Indexer{
		offset: 0x00,
	}, i.Schema().Indexer)

	requireT.Panics(func() {
		NewFieldIndex(v, &v.Value2)
	})

	i = NewFieldIndex(v, &v.Value2.Value1)
	requireT.Equal(uint64Indexer{
		offset: 0x08,
	}, i.Schema().Indexer)

	requireT.Panics(func() {
		NewFieldIndex(v, &v.Value2.Value2)
	})

	i = NewFieldIndex(v, &v.Value2.Value2.Value1)
	requireT.Equal(stringIndexer{
		offset: 0x70,
	}, i.Schema().Indexer)

	i = NewFieldIndex(v, &v.Value2.Value2.Value2)
	requireT.Equal(int16Indexer{
		offset: 0x80,
	}, i.Schema().Indexer)

	i = NewFieldIndex(v, &v.Value2.Value2.Value3)
	requireT.Equal(uint8Indexer{
		offset: 0x82,
	}, i.Schema().Indexer)

	i = NewFieldIndex(v, &v.Value2.Value3)
	requireT.Equal(stringIndexer{
		offset: 0x88,
	}, i.Schema().Indexer)

	requireT.Panics(func() {
		NewFieldIndex(v, &v.Value3)
	})

	i = NewFieldIndex(v, &v.Value3.Value1)
	requireT.Equal(stringIndexer{
		offset: 0xf8,
	}, i.Schema().Indexer)

	i = NewFieldIndex(v, &v.Value3.Value2)
	requireT.Equal(int16Indexer{
		offset: 0x108,
	}, i.Schema().Indexer)

	i = NewFieldIndex(v, &v.Value3.Value3)
	requireT.Equal(uint8Indexer{
		offset: 0x10a,
	}, i.Schema().Indexer)

	i = NewFieldIndex(v, &v.Value4)
	requireT.Equal(stringIndexer{
		offset: 0x110,
	}, i.Schema().Indexer)
}

func TestIndexerOffset0(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &subO2{}

	index := NewFieldIndex(v, &v.ValueBool)
	indexer := index.Schema().Indexer.(boolIndexer)

	v.ValueBool = false
	verify(requireT, indexer, []byte{0x00}, v, v.ValueBool)

	v.ValueBool = true
	verify(requireT, indexer, []byte{0x01}, v, v.ValueBool)
}

func TestBoolIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueBool)
	requireT.False(index.Schema().Unique)
	indexer := index.Schema().Indexer.(boolIndexer)

	v.Value2.Value2.ValueBool = false
	verify(requireT, indexer, []byte{0x00}, v, v.Value2.Value2.ValueBool)

	v.Value2.Value2.ValueBool = true
	verify(requireT, indexer, []byte{0x01}, v, v.Value2.Value2.ValueBool)
}

func TestStringIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueString)
	indexer := index.Schema().Indexer.(stringIndexer)

	v.Value2.Value2.ValueString = ""
	verify(requireT, indexer, []byte{0x00}, v, v.Value2.Value2.ValueString)

	v.Value2.Value2.ValueString = abc
	verify(requireT, indexer, []byte{0x41, 0x42, 0x43, 0x00}, v, v.Value2.Value2.ValueString)
}

func TestTimeIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueTime)
	indexer := index.Schema().Indexer.(timeIndexer)

	v.Value2.Value2.ValueTime = time.Time{}
	verify(requireT, indexer, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		v, v.Value2.Value2.ValueTime)

	v.Value2.Value2.ValueTime = time.Date(2024, 1, 1, 1, 1, 1, 99999, time.UTC)
	verify(requireT, indexer, []byte{0x80, 0x0, 0x0, 0xe, 0xdd, 0x24, 0x5, 0xcd, 0x0, 0x1, 0x86, 0x9f},
		v, v.Value2.Value2.ValueTime)

	v.Value2.Value2.ValueTime = time.Date(-1, 1, 1, 1, 1, 1, 99999, time.UTC)
	verify(requireT, indexer, []byte{0x7f, 0xff, 0xff, 0xff, 0xfc, 0x3c, 0x55, 0xcd, 0x0, 0x1, 0x86, 0x9f},
		v, v.Value2.Value2.ValueTime)
}

func TestInt8Indexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueInt8)
	indexer := index.Schema().Indexer.(int8Indexer)

	v.Value2.Value2.ValueInt8 = 0
	verify(requireT, indexer, []byte{0x80}, v, v.Value2.Value2.ValueInt8)

	v.Value2.Value2.ValueInt8 = 127
	verify(requireT, indexer, []byte{0xff}, v, v.Value2.Value2.ValueInt8)

	v.Value2.Value2.ValueInt8 = -128
	verify(requireT, indexer, []byte{0x00}, v, v.Value2.Value2.ValueInt8)
}

func TestInt16Indexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueInt16)
	indexer := index.Schema().Indexer.(int16Indexer)

	v.Value2.Value2.ValueInt16 = 0
	verify(requireT, indexer, []byte{0x80, 0x00}, v, v.Value2.Value2.ValueInt16)

	v.Value2.Value2.ValueInt16 = 30000
	verify(requireT, indexer, []byte{0xf5, 0x30}, v, v.Value2.Value2.ValueInt16)

	v.Value2.Value2.ValueInt16 = -30000
	verify(requireT, indexer, []byte{0x0a, 0xd0}, v, v.Value2.Value2.ValueInt16)
}

func TestInt32Indexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueInt32)
	indexer := index.Schema().Indexer.(int32Indexer)

	v.Value2.Value2.ValueInt32 = 0
	verify(requireT, indexer, []byte{0x80, 0x00, 0x00, 0x00}, v, v.Value2.Value2.ValueInt32)

	v.Value2.Value2.ValueInt32 = 300000000
	verify(requireT, indexer, []byte{0x91, 0xe1, 0xa3, 0x0}, v, v.Value2.Value2.ValueInt32)

	v.Value2.Value2.ValueInt32 = -300000000
	verify(requireT, indexer, []byte{0x6e, 0x1e, 0x5d, 0x0}, v, v.Value2.Value2.ValueInt32)
}

func TestInt64Indexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueInt64)
	indexer := index.Schema().Indexer.(int64Indexer)

	v.Value2.Value2.ValueInt64 = 0
	verify(requireT, indexer, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, v, v.Value2.Value2.ValueInt64)

	v.Value2.Value2.ValueInt64 = 3000000000000000000
	verify(requireT, indexer, []byte{0xa9, 0xa2, 0x24, 0x1a, 0xf6, 0x2c, 0x0, 0x0}, v, v.Value2.Value2.ValueInt64)

	v.Value2.Value2.ValueInt64 = -3000000000000000000
	verify(requireT, indexer, []byte{0x56, 0x5d, 0xdb, 0xe5, 0x9, 0xd4, 0x0, 0x0}, v, v.Value2.Value2.ValueInt64)
}

func TestUInt8Indexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueUint8)
	indexer := index.Schema().Indexer.(uint8Indexer)

	v.Value2.Value2.ValueUint8 = 0
	verify(requireT, indexer, []byte{0x00}, v, v.Value2.Value2.ValueUint8)

	v.Value2.Value2.ValueUint8 = math.MaxUint8
	verify(requireT, indexer, []byte{0xff}, v, v.Value2.Value2.ValueUint8)
}

func TestUInt16Indexer(t *testing.T) {
	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueUint16)
	indexer := index.Schema().Indexer.(uint16Indexer)

	v.Value2.Value2.ValueUint16 = 0
	verify(requireT, indexer, []byte{0x00, 0x00}, v, v.Value2.Value2.ValueUint16)

	v.Value2.Value2.ValueUint16 = math.MaxUint16
	verify(requireT, indexer, []byte{0xff, 0xff}, v, v.Value2.Value2.ValueUint16)
}

func TestUInt32Indexer(t *testing.T) {
	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueUint32)
	indexer := index.Schema().Indexer.(uint32Indexer)

	v.Value2.Value2.ValueUint32 = 0
	verify(requireT, indexer, []byte{0x00, 0x00, 0x00, 0x00}, v, v.Value2.Value2.ValueUint32)

	v.Value2.Value2.ValueUint32 = math.MaxUint32
	verify(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff}, v, v.Value2.Value2.ValueUint32)
}

func TestUInt64Indexer(t *testing.T) {
	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueUint64)
	indexer := index.Schema().Indexer.(uint64Indexer)

	v.Value2.Value2.ValueUint64 = 0
	verify(requireT, indexer, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, v, v.Value2.Value2.ValueUint64)

	v.Value2.Value2.ValueUint64 = math.MaxUint64
	verify(requireT, indexer, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, v, v.Value2.Value2.ValueUint64)
}

func TestIDIndexer(t *testing.T) {
	requireT := require.New(t)
	v := &o{}

	index := NewFieldIndex(v, &v.Value2.Value2.ValueID)
	indexer := index.Schema().Indexer.(idIndexer)

	v.Value2.Value2.ValueID = id.ID{}
	verify(requireT, indexer, []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}, v, v.Value2.Value2.ValueID)

	v.Value2.Value2.ValueID = id.ID{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}
	verify(requireT, indexer, []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf},
		v, v.Value2.Value2.ValueID)
}
