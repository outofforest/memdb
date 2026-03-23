package indices

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// FieldIndex defines index indexing entities by struct field.
type FieldIndex[T any] struct {
	id      uint64
	indexer memdb.Indexer
}

// NewFieldIndex defines new field index.
func NewFieldIndex[T any, F fieldConstraint](ePtr *T, fieldPtr *F) *FieldIndex[T] {
	var _ Index[T] = (*FieldIndex[T])(nil)

	eType := reflect.TypeFor[T]()
	if eType.Kind() != reflect.Struct {
		panic(errors.New("*ePtr is not a struct"))
	}

	fieldType := reflect.TypeFor[F]()

	eStart := reflect.ValueOf(ePtr).Pointer()
	eSize := eType.Size()
	fieldStart := reflect.ValueOf(fieldPtr).Pointer()
	if fieldStart < eStart || fieldStart >= eStart+eSize {
		panic(errors.Errorf("field does not belong to entity"))
	}

	offset := fieldStart - eStart
	foundFieldType := findField(eType, offset)
	indexer, err := indexerForType(fieldType, offset)
	if err != nil {
		panic(err)
	}
	if foundFieldType != fieldType {
		panic(errors.Errorf("unexpected field type %s, expected %s", foundFieldType, fieldType))
	}

	index := &FieldIndex[T]{
		indexer: indexer,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// ID returns ID of the index.
func (i *FieldIndex[T]) ID() uint64 {
	return i.id
}

// Schema returns memdb index schema.
func (i *FieldIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Indexer: i.indexer,
	}
}

// Type returns type of entity index is created for.
func (i *FieldIndex[T]) Type() reflect.Type {
	return reflect.TypeFor[T]()
}

func (i *FieldIndex[T]) dummyTDefiner(t T) {
	panic("it should never be called")
}

func findField(t reflect.Type, offset uintptr) reflect.Type {
	var field reflect.StructField
	for {
		for i := range t.NumField() {
			f := t.Field(i)
			if f.Offset > offset {
				break
			}
			field = f
		}

		if field.Type.Kind() != reflect.Struct || field.Type.ConvertibleTo(timeType) {
			return field.Type
		}
		offset -= field.Offset
		t = field.Type
	}
}

func valueByOffset[T any](o unsafe.Pointer, offset uintptr) T {
	return *(*T)(unsafe.Pointer(uintptr(o) + offset))
}

func boolToBytes(v bool, b []byte) {
	if v {
		b[0] = 0x01
	}
}

var _ memdb.Indexer = &boolIndexer{}
var _ memdb.ArgSerializer = &boolIndexer{}

type boolIndexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *boolIndexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *boolIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 1
}

func (i *boolIndexer) SizeFromArg(arg any) uint64 {
	return 1
}

func (i *boolIndexer) FromArg(b []byte, arg any) uint64 {
	boolToBytes(reflect.ValueOf(arg).Bool(), b)
	return 1
}

func (i *boolIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	boolToBytes(valueByOffset[bool](o, i.offset), b)
	return 1
}

func stringToBytes(s string, b []byte) uint64 {
	return uint64(copy(b, s)) + 1
}

var _ memdb.Indexer = &stringIndexer{}
var _ memdb.ArgSerializer = &stringIndexer{}

type stringIndexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *stringIndexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *stringIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return uint64(len(valueByOffset[string](o, i.offset))) + 1
}

func (i *stringIndexer) SizeFromArg(arg any) uint64 {
	return uint64(len(reflect.ValueOf(arg).String())) + 1
}

func (i *stringIndexer) FromArg(b []byte, arg any) uint64 {
	return stringToBytes(reflect.ValueOf(arg).String(), b)
}

func (i *stringIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	return stringToBytes(valueByOffset[string](o, i.offset), b)
}

var (
	secondsOffset = time.Time{}.Unix()
	timeType      = reflect.TypeFor[time.Time]()
)

func timeToBytes(t time.Time, b []byte) {
	binary.BigEndian.PutUint64(b, uint64(t.Unix()-secondsOffset)^0x8000000000000000)
	binary.BigEndian.PutUint32(b[8:], uint32(t.Nanosecond()))
}

var _ memdb.Indexer = &timeIndexer{}
var _ memdb.ArgSerializer = &timeIndexer{}

type timeIndexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *timeIndexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *timeIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 12
}

func (i *timeIndexer) SizeFromArg(arg any) uint64 {
	return 12
}

func (i *timeIndexer) FromArg(b []byte, arg any) uint64 {
	timeToBytes(reflect.ValueOf(arg).Convert(timeType).Interface().(time.Time), b)
	return 12
}

func (i *timeIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	timeToBytes(valueByOffset[time.Time](o, i.offset), b)
	return 12
}

func int8ToBytes(i int8, b []byte) {
	b[0] = uint8(i) ^ 0x80
}

var _ memdb.Indexer = &int8Indexer{}
var _ memdb.ArgSerializer = &int8Indexer{}

type int8Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *int8Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *int8Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 1
}

func (i *int8Indexer) SizeFromArg(arg any) uint64 {
	return 1
}

func (i *int8Indexer) FromArg(b []byte, arg any) uint64 {
	int8ToBytes(int8(reflect.ValueOf(arg).Int()), b)
	return 1
}

func (i *int8Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int8ToBytes(valueByOffset[int8](o, i.offset), b)
	return 1
}

func int16ToBytes(i int16, b []byte) {
	binary.BigEndian.PutUint16(b, uint16(i)^0x8000)
}

var _ memdb.Indexer = &int16Indexer{}
var _ memdb.Indexer = &int16Indexer{}

type int16Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *int16Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *int16Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 2
}

func (i *int16Indexer) SizeFromArg(arg any) uint64 {
	return 2
}

func (i *int16Indexer) FromArg(b []byte, arg any) uint64 {
	int16ToBytes(int16(reflect.ValueOf(arg).Int()), b)
	return 2
}

func (i *int16Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int16ToBytes(valueByOffset[int16](o, i.offset), b)
	return 2
}

func int32ToBytes(i int32, b []byte) {
	binary.BigEndian.PutUint32(b, uint32(i)^0x80000000)
}

var _ memdb.Indexer = &int32Indexer{}
var _ memdb.ArgSerializer = &int32Indexer{}

type int32Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *int32Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *int32Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 4
}

func (i *int32Indexer) SizeFromArg(arg any) uint64 {
	return 4
}

func (i *int32Indexer) FromArg(b []byte, arg any) uint64 {
	int32ToBytes(int32(reflect.ValueOf(arg).Int()), b)
	return 4
}

func (i *int32Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int32ToBytes(valueByOffset[int32](o, i.offset), b)
	return 4
}

func int64ToBytes(i int64, b []byte) {
	binary.BigEndian.PutUint64(b, uint64(i)^0x8000000000000000)
}

var _ memdb.Indexer = &int64Indexer{}
var _ memdb.ArgSerializer = &int64Indexer{}

type int64Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *int64Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *int64Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 8
}

func (i *int64Indexer) SizeFromArg(arg any) uint64 {
	return 8
}

func (i *int64Indexer) FromArg(b []byte, arg any) uint64 {
	int64ToBytes(reflect.ValueOf(arg).Int(), b)
	return 8
}

func (i *int64Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int64ToBytes(valueByOffset[int64](o, i.offset), b)
	return 8
}

func uint8ToBytes(i uint8, b []byte) {
	b[0] = i
}

var _ memdb.Indexer = &uint8Indexer{}
var _ memdb.ArgSerializer = &uint8Indexer{}

type uint8Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *uint8Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *uint8Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 1
}

func (i *uint8Indexer) SizeFromArg(arg any) uint64 {
	return 1
}

func (i *uint8Indexer) FromArg(b []byte, arg any) uint64 {
	uint8ToBytes(uint8(reflect.ValueOf(arg).Uint()), b)
	return 1
}

func (i *uint8Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint8ToBytes(valueByOffset[uint8](o, i.offset), b)
	return 1
}

func uint16ToBytes(i uint16, b []byte) {
	binary.BigEndian.PutUint16(b, i)
}

var _ memdb.Indexer = &uint16Indexer{}
var _ memdb.ArgSerializer = &uint16Indexer{}

type uint16Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *uint16Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *uint16Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 2
}

func (i *uint16Indexer) SizeFromArg(arg any) uint64 {
	return 2
}

func (i *uint16Indexer) FromArg(b []byte, arg any) uint64 {
	uint16ToBytes(uint16(reflect.ValueOf(arg).Uint()), b)
	return 2
}

func (i *uint16Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint16ToBytes(valueByOffset[uint16](o, i.offset), b)
	return 2
}

func uint32ToBytes(i uint32, b []byte) {
	binary.BigEndian.PutUint32(b, i)
}

var _ memdb.Indexer = &uint32Indexer{}
var _ memdb.ArgSerializer = &uint32Indexer{}

type uint32Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *uint32Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *uint32Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 4
}

func (i *uint32Indexer) SizeFromArg(arg any) uint64 {
	return 4
}

func (i *uint32Indexer) FromArg(b []byte, arg any) uint64 {
	uint32ToBytes(uint32(reflect.ValueOf(arg).Uint()), b)
	return 4
}

func (i *uint32Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint32ToBytes(valueByOffset[uint32](o, i.offset), b)
	return 4
}

func uint64ToBytes(i uint64, b []byte) {
	binary.BigEndian.PutUint64(b, i)
}

var _ memdb.Indexer = &uint64Indexer{}
var _ memdb.ArgSerializer = &uint64Indexer{}

type uint64Indexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *uint64Indexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *uint64Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 8
}

func (i *uint64Indexer) SizeFromArg(arg any) uint64 {
	return 8
}

func (i *uint64Indexer) FromArg(b []byte, arg any) uint64 {
	uint64ToBytes(reflect.ValueOf(arg).Uint(), b)
	return 8
}

func (i *uint64Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint64ToBytes(valueByOffset[uint64](o, i.offset), b)
	return 8
}

var idType = reflect.TypeFor[memdb.ID]()

var _ memdb.Indexer = &idIndexer{}
var _ memdb.Indexer = &idIndexer{}

func idToBytes(id memdb.ID, b []byte) {
	copy(b, id[:])
}

type idIndexer struct {
	offset uintptr
	args   []memdb.ArgSerializer
}

func (i *idIndexer) Args() []memdb.ArgSerializer {
	return i.args
}

func (i *idIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return memdb.IDLength
}

func (i *idIndexer) SizeFromArg(arg any) uint64 {
	return memdb.IDLength
}

func (i *idIndexer) FromArg(b []byte, arg any) uint64 {
	idToBytes(reflect.ValueOf(arg).Convert(idType).Interface().(memdb.ID), b)
	return memdb.IDLength
}

func (i *idIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	idToBytes(valueByOffset[memdb.ID](o, i.offset), b)
	return memdb.IDLength
}

func indexerForType(t reflect.Type, offset uintptr) (memdb.Indexer, error) {
	if t.ConvertibleTo(idType) {
		i := &idIndexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	}
	if t.ConvertibleTo(timeType) {
		i := &timeIndexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	}
	switch t.Kind() {
	case reflect.Bool:
		i := &boolIndexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.String:
		i := &stringIndexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Int8:
		i := &int8Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Int16:
		i := &int16Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Int32:
		i := &int32Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Int64:
		i := &int64Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Uint8:
		i := &uint8Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Uint16:
		i := &uint16Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Uint32:
		i := &uint32Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	case reflect.Uint64:
		i := &uint64Indexer{offset: offset}
		i.args = []memdb.ArgSerializer{i}
		return i, nil
	default:
		return nil, errors.Errorf("unsupported type: %s", t)
	}
}

type fieldConstraint interface {
	//nolint:lll
	~[memdb.IDLength]byte | time.Time | ~bool | ~string | ~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}
