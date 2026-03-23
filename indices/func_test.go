package indices

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestFuncIndexType(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex1(func(e *o) *uint8 {
		return lo.ToPtr[uint8](0)
	})

	requireT.Equal(reflect.TypeFor[o](), i.Type())
}

func TestFuncIndex1(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex1(func(e *o) *uint8 {
		return lo.ToPtr[uint8](0xff)
	})

	b := make([]byte, 2)
	requireT.EqualValues(1, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(1, size)
	requireT.Equal([]byte{0xff, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 1)
	requireT.EqualValues(1, args[0].SizeFromArg(uint8(0)))
}

func TestFuncIndex2(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex2(func(e *o) (*uint8, *uint16) {
		return lo.ToPtr[uint8](0xff),
			lo.ToPtr[uint16](0xeeee)
	})

	b := make([]byte, 4)
	requireT.EqualValues(3, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(3, size)
	requireT.Equal([]byte{0xff, 0xee, 0xee, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 2)
	requireT.EqualValues(1, args[0].SizeFromArg(uint8(0)))
	requireT.EqualValues(2, args[1].SizeFromArg(uint16(0)))
}

func TestFuncIndex3(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex3(func(e *o) (*uint8, *uint16, *uint32) {
		return lo.ToPtr[uint8](0xff),
			lo.ToPtr[uint16](0xeeee),
			lo.ToPtr[uint32](0xdddddddd)
	})

	b := make([]byte, 8)
	requireT.EqualValues(7, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(7, size)
	requireT.Equal([]byte{0xff, 0xee, 0xee, 0xdd, 0xdd, 0xdd, 0xdd, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 3)
	requireT.EqualValues(1, args[0].SizeFromArg(uint8(0)))
	requireT.EqualValues(2, args[1].SizeFromArg(uint16(0)))
	requireT.EqualValues(4, args[2].SizeFromArg(uint32(0)))
}

func TestFuncIndex4(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex4(func(e *o) (*uint8, *uint16, *uint32, *uint64) {
		return lo.ToPtr[uint8](0xff),
			lo.ToPtr[uint16](0xeeee),
			lo.ToPtr[uint32](0xdddddddd),
			lo.ToPtr[uint64](0xcccccccccccccccc)
	})

	b := make([]byte, 16)
	requireT.EqualValues(15, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(15, size)
	requireT.Equal([]byte{0xff, 0xee, 0xee, 0xdd, 0xdd, 0xdd, 0xdd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc,
		0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 4)
	requireT.EqualValues(1, args[0].SizeFromArg(uint8(0)))
	requireT.EqualValues(2, args[1].SizeFromArg(uint16(0)))
	requireT.EqualValues(4, args[2].SizeFromArg(uint32(0)))
	requireT.EqualValues(8, args[3].SizeFromArg(uint64(0)))
}

func TestFuncIndex5(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex5(func(e *o) (*uint8, *uint16, *uint32, *uint64, *uint8) {
		return lo.ToPtr[uint8](0xff),
			lo.ToPtr[uint16](0xeeee),
			lo.ToPtr[uint32](0xdddddddd),
			lo.ToPtr[uint64](0xcccccccccccccccc),
			lo.ToPtr[uint8](0xbb)
	})

	b := make([]byte, 17)
	requireT.EqualValues(16, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(16, size)
	requireT.Equal([]byte{0xff, 0xee, 0xee, 0xdd, 0xdd, 0xdd, 0xdd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc,
		0xbb, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 5)
	requireT.EqualValues(1, args[0].SizeFromArg(uint8(0)))
	requireT.EqualValues(2, args[1].SizeFromArg(uint16(0)))
	requireT.EqualValues(4, args[2].SizeFromArg(uint32(0)))
	requireT.EqualValues(8, args[3].SizeFromArg(uint64(0)))
	requireT.EqualValues(1, args[4].SizeFromArg(uint8(0)))
}

func TestFuncIndex6(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex6(func(e *o) (*uint8, *uint16, *uint32, *uint64, *uint8, *uint16) {
		return lo.ToPtr[uint8](0xff),
			lo.ToPtr[uint16](0xeeee),
			lo.ToPtr[uint32](0xdddddddd),
			lo.ToPtr[uint64](0xcccccccccccccccc),
			lo.ToPtr[uint8](0xbb),
			lo.ToPtr[uint16](0xaaaa)
	})

	b := make([]byte, 19)
	requireT.EqualValues(18, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(18, size)
	requireT.Equal([]byte{0xff, 0xee, 0xee, 0xdd, 0xdd, 0xdd, 0xdd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc,
		0xbb, 0xaa, 0xaa, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 6)
	requireT.EqualValues(1, args[0].SizeFromArg(uint8(0)))
	requireT.EqualValues(2, args[1].SizeFromArg(uint16(0)))
	requireT.EqualValues(4, args[2].SizeFromArg(uint32(0)))
	requireT.EqualValues(8, args[3].SizeFromArg(uint64(0)))
	requireT.EqualValues(1, args[4].SizeFromArg(uint8(0)))
	requireT.EqualValues(2, args[5].SizeFromArg(uint16(0)))
}

func TestFuncIndex1Strings(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex1(func(e *o) *string {
		return lo.ToPtr("A")
	})

	b := make([]byte, 3)
	requireT.EqualValues(2, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(2, size)
	requireT.Equal([]byte{0x41, 0x0, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 1)
	requireT.EqualValues(2, args[0].SizeFromArg("A"))
}

func TestFuncIndex2Strings(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex2(func(e *o) (*string, *string) {
		return lo.ToPtr("A"),
			lo.ToPtr("BB")
	})

	b := make([]byte, 6)
	requireT.EqualValues(5, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(5, size)
	requireT.Equal([]byte{0x41, 0x0, 0x42, 0x42, 0x00, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 2)
	requireT.EqualValues(2, args[0].SizeFromArg("A"))
	requireT.EqualValues(3, args[1].SizeFromArg("BB"))
}

func TestFuncIndex3Strings(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex3(func(e *o) (*string, *string, *string) {
		return lo.ToPtr("A"),
			lo.ToPtr("BB"),
			lo.ToPtr("CCC")
	})

	b := make([]byte, 10)
	requireT.EqualValues(9, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(9, size)
	requireT.Equal([]byte{0x41, 0x0, 0x42, 0x42, 0x00, 0x43, 0x43, 0x43, 0x00, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 3)
	requireT.EqualValues(2, args[0].SizeFromArg("A"))
	requireT.EqualValues(3, args[1].SizeFromArg("BB"))
	requireT.EqualValues(4, args[2].SizeFromArg("CCC"))
}

func TestFuncIndex4Strings(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex4(func(e *o) (*string, *string, *string, *string) {
		return lo.ToPtr("A"),
			lo.ToPtr("BB"),
			lo.ToPtr("CCC"),
			lo.ToPtr("DDDD")
	})

	b := make([]byte, 15)
	requireT.EqualValues(14, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(14, size)
	requireT.Equal([]byte{0x41, 0x0, 0x42, 0x42, 0x00, 0x43, 0x43, 0x43, 0x00, 0x44, 0x44, 0x44, 0x44, 0x00, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 4)
	requireT.EqualValues(2, args[0].SizeFromArg("A"))
	requireT.EqualValues(3, args[1].SizeFromArg("BB"))
	requireT.EqualValues(4, args[2].SizeFromArg("CCC"))
	requireT.EqualValues(5, args[3].SizeFromArg("DDDD"))
}

func TestFuncIndex5Strings(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex5(func(e *o) (*string, *string, *string, *string, *string) {
		return lo.ToPtr("A"),
			lo.ToPtr("BB"),
			lo.ToPtr("CCC"),
			lo.ToPtr("DDDD"),
			lo.ToPtr("EEEEE")
	})

	b := make([]byte, 21)
	requireT.EqualValues(20, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(20, size)
	requireT.Equal([]byte{0x41, 0x0, 0x42, 0x42, 0x00, 0x43, 0x43, 0x43, 0x00, 0x44, 0x44, 0x44, 0x44, 0x00,
		0x45, 0x45, 0x45, 0x45, 0x45, 0x00, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 5)
	requireT.EqualValues(2, args[0].SizeFromArg("A"))
	requireT.EqualValues(3, args[1].SizeFromArg("BB"))
	requireT.EqualValues(4, args[2].SizeFromArg("CCC"))
	requireT.EqualValues(5, args[3].SizeFromArg("DDDD"))
	requireT.EqualValues(6, args[4].SizeFromArg("EEEEE"))
}

func TestFuncIndex6Strings(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	i := NewFuncIndex6(func(e *o) (*string, *string, *string, *string, *string, *string) {
		return lo.ToPtr("A"),
			lo.ToPtr("BB"),
			lo.ToPtr("CCC"),
			lo.ToPtr("DDDD"),
			lo.ToPtr("EEEEE"),
			lo.ToPtr("FFFFFF")
	})

	b := make([]byte, 28)
	requireT.EqualValues(27, i.Schema().Indexer.SizeFromObject(unsafe.Pointer(&o{})))
	size := i.Schema().Indexer.FromObject(b, unsafe.Pointer(&o{}))
	requireT.EqualValues(27, size)
	requireT.Equal([]byte{0x41, 0x0, 0x42, 0x42, 0x00, 0x43, 0x43, 0x43, 0x00, 0x44, 0x44, 0x44, 0x44, 0x00,
		0x45, 0x45, 0x45, 0x45, 0x45, 0x00, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x00, 0x0}, b)

	args := i.Schema().Indexer.Args()
	requireT.Len(args, 6)
	requireT.EqualValues(2, args[0].SizeFromArg("A"))
	requireT.EqualValues(3, args[1].SizeFromArg("BB"))
	requireT.EqualValues(4, args[2].SizeFromArg("CCC"))
	requireT.EqualValues(5, args[3].SizeFromArg("DDDD"))
	requireT.EqualValues(6, args[4].SizeFromArg("EEEEE"))
	requireT.EqualValues(7, args[5].SizeFromArg("FFFFFF"))
}
