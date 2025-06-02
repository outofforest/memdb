package indices

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// NewFieldIndex defines new field index.
func NewFieldIndex(ePtr, fieldPtr any) *FieldIndex {
	ePtrType := reflect.TypeOf(ePtr)
	if ePtrType.Kind() != reflect.Ptr {
		panic(errors.New("ePtr is not a pointer"))
	}
	if ePtrType.Elem().Kind() != reflect.Struct {
		panic(errors.New("*ePtr is not a struct"))
	}

	fieldPtrType := reflect.TypeOf(fieldPtr)
	if fieldPtrType.Kind() != reflect.Ptr {
		panic(errors.New("fieldPtr is not a pointer"))
	}
	if fieldPtrType.Elem().Kind() == reflect.Ptr {
		panic(errors.New("field is a pointer"))
	}

	eStart := reflect.ValueOf(ePtr).Pointer()
	eSize := ePtrType.Elem().Size()
	fieldStart := reflect.ValueOf(fieldPtr).Pointer()
	if fieldStart < eStart || fieldStart >= eStart+eSize {
		panic(errors.Errorf("field does not belong to entity"))
	}

	offset := fieldStart - eStart
	fieldType := findField(ePtrType.Elem(), offset)
	indexer, err := indexerForType(fieldType, offset)
	if err != nil {
		panic(err)
	}
	if fieldType != fieldPtrType.Elem() {
		panic(errors.Errorf("unexpected field type %s, expected %s", fieldType, fieldPtrType.Elem()))
	}

	index := &FieldIndex{
		entityType: ePtrType.Elem(),
		indexer:    indexer,
	}
	index.id = uint64(uintptr(unsafe.Pointer(index)))
	return index
}

// FieldIndex defines index indexing entities by struct field.
type FieldIndex struct {
	id         uint64
	entityType reflect.Type
	indexer    memdb.Indexer
}

// ID returns ID of the index.
func (i *FieldIndex) ID() uint64 {
	return i.id
}

// Type returns type of entity index is defined for.
func (i *FieldIndex) Type() reflect.Type {
	return i.entityType
}

// Schema returns memdb index schema.
func (i *FieldIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Indexer: i.indexer,
	}
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

var _ memdb.Indexer = boolIndexer{}
var _ memdb.ArgSerializer = boolIndexer{}

type boolIndexer struct {
	offset uintptr
}

func (i boolIndexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i boolIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 1
}

func (i boolIndexer) SizeFromArg(arg any) uint64 {
	return 1
}

func (i boolIndexer) FromArg(b []byte, arg any) uint64 {
	boolToBytes(reflect.ValueOf(arg).Bool(), b)
	return 1
}

func (i boolIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	boolToBytes(valueByOffset[bool](o, i.offset), b)
	return 1
}

func stringToBytes(s string, b []byte) uint64 {
	return uint64(copy(b, s)) + 1
}

var _ memdb.Indexer = stringIndexer{}
var _ memdb.ArgSerializer = stringIndexer{}

type stringIndexer struct {
	offset uintptr
}

func (i stringIndexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i stringIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return uint64(len(valueByOffset[string](o, i.offset))) + 1
}

func (i stringIndexer) SizeFromArg(arg any) uint64 {
	return uint64(len(reflect.ValueOf(arg).String())) + 1
}

func (i stringIndexer) FromArg(b []byte, arg any) uint64 {
	return stringToBytes(reflect.ValueOf(arg).String(), b)
}

func (i stringIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	return stringToBytes(valueByOffset[string](o, i.offset), b)
}

var (
	secondsOffset = time.Time{}.Unix()
	timeType      = reflect.TypeOf(time.Time{})
)

func timeToBytes(t time.Time, b []byte) {
	binary.BigEndian.PutUint64(b, uint64(t.Unix()-secondsOffset)^0x8000000000000000)
	binary.BigEndian.PutUint32(b[8:], uint32(t.Nanosecond()))
}

var _ memdb.Indexer = timeIndexer{}
var _ memdb.ArgSerializer = timeIndexer{}

type timeIndexer struct {
	offset uintptr
}

func (i timeIndexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i timeIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 12
}

func (i timeIndexer) SizeFromArg(arg any) uint64 {
	return 12
}

func (i timeIndexer) FromArg(b []byte, arg any) uint64 {
	timeToBytes(reflect.ValueOf(arg).Convert(timeType).Interface().(time.Time), b)
	return 12
}

func (i timeIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	timeToBytes(valueByOffset[time.Time](o, i.offset), b)
	return 12
}

func int8ToBytes(i int8, b []byte) {
	b[0] = uint8(i) ^ 0x80
}

var _ memdb.Indexer = int8Indexer{}
var _ memdb.ArgSerializer = int8Indexer{}

type int8Indexer struct {
	offset uintptr
}

func (i int8Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i int8Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 1
}

func (i int8Indexer) SizeFromArg(arg any) uint64 {
	return 1
}

func (i int8Indexer) FromArg(b []byte, arg any) uint64 {
	int8ToBytes(int8(reflect.ValueOf(arg).Int()), b)
	return 1
}

func (i int8Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int8ToBytes(valueByOffset[int8](o, i.offset), b)
	return 1
}

func int16ToBytes(i int16, b []byte) {
	binary.BigEndian.PutUint16(b, uint16(i)^0x8000)
}

var _ memdb.Indexer = int16Indexer{}
var _ memdb.Indexer = int16Indexer{}

type int16Indexer struct {
	offset uintptr
}

func (i int16Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i int16Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 2
}

func (i int16Indexer) SizeFromArg(arg any) uint64 {
	return 2
}

func (i int16Indexer) FromArg(b []byte, arg any) uint64 {
	int16ToBytes(int16(reflect.ValueOf(arg).Int()), b)
	return 2
}

func (i int16Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int16ToBytes(valueByOffset[int16](o, i.offset), b)
	return 2
}

func int32ToBytes(i int32, b []byte) {
	binary.BigEndian.PutUint32(b, uint32(i)^0x80000000)
}

var _ memdb.Indexer = int32Indexer{}
var _ memdb.ArgSerializer = int32Indexer{}

type int32Indexer struct {
	offset uintptr
}

func (i int32Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i int32Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 4
}

func (i int32Indexer) SizeFromArg(arg any) uint64 {
	return 4
}

func (i int32Indexer) FromArg(b []byte, arg any) uint64 {
	int32ToBytes(int32(reflect.ValueOf(arg).Int()), b)
	return 4
}

func (i int32Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int32ToBytes(valueByOffset[int32](o, i.offset), b)
	return 4
}

func int64ToBytes(i int64, b []byte) {
	binary.BigEndian.PutUint64(b, uint64(i)^0x8000000000000000)
}

var _ memdb.Indexer = int64Indexer{}
var _ memdb.ArgSerializer = int64Indexer{}

type int64Indexer struct {
	offset uintptr
}

func (i int64Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i int64Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 8
}

func (i int64Indexer) SizeFromArg(arg any) uint64 {
	return 8
}

func (i int64Indexer) FromArg(b []byte, arg any) uint64 {
	int64ToBytes(reflect.ValueOf(arg).Int(), b)
	return 8
}

func (i int64Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	int64ToBytes(valueByOffset[int64](o, i.offset), b)
	return 8
}

func uint8ToBytes(i uint8, b []byte) {
	b[0] = i
}

var _ memdb.Indexer = uint8Indexer{}
var _ memdb.ArgSerializer = uint8Indexer{}

type uint8Indexer struct {
	offset uintptr
}

func (i uint8Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i uint8Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 1
}

func (i uint8Indexer) SizeFromArg(arg any) uint64 {
	return 1
}

func (i uint8Indexer) FromArg(b []byte, arg any) uint64 {
	uint8ToBytes(uint8(reflect.ValueOf(arg).Uint()), b)
	return 1
}

func (i uint8Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint8ToBytes(valueByOffset[uint8](o, i.offset), b)
	return 1
}

func uint16ToBytes(i uint16, b []byte) {
	binary.BigEndian.PutUint16(b, i)
}

var _ memdb.Indexer = uint16Indexer{}
var _ memdb.ArgSerializer = uint16Indexer{}

type uint16Indexer struct {
	offset uintptr
}

func (i uint16Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i uint16Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 2
}

func (i uint16Indexer) SizeFromArg(arg any) uint64 {
	return 2
}

func (i uint16Indexer) FromArg(b []byte, arg any) uint64 {
	uint16ToBytes(uint16(reflect.ValueOf(arg).Uint()), b)
	return 2
}

func (i uint16Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint16ToBytes(valueByOffset[uint16](o, i.offset), b)
	return 2
}

func uint32ToBytes(i uint32, b []byte) {
	binary.BigEndian.PutUint32(b, i)
}

var _ memdb.Indexer = uint32Indexer{}
var _ memdb.ArgSerializer = uint32Indexer{}

type uint32Indexer struct {
	offset uintptr
}

func (i uint32Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i uint32Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 4
}

func (i uint32Indexer) SizeFromArg(arg any) uint64 {
	return 4
}

func (i uint32Indexer) FromArg(b []byte, arg any) uint64 {
	uint32ToBytes(uint32(reflect.ValueOf(arg).Uint()), b)
	return 4
}

func (i uint32Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint32ToBytes(valueByOffset[uint32](o, i.offset), b)
	return 4
}

func uint64ToBytes(i uint64, b []byte) {
	binary.BigEndian.PutUint64(b, i)
}

var _ memdb.Indexer = uint64Indexer{}
var _ memdb.ArgSerializer = uint64Indexer{}

type uint64Indexer struct {
	offset uintptr
}

func (i uint64Indexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i uint64Indexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return 8
}

func (i uint64Indexer) SizeFromArg(arg any) uint64 {
	return 8
}

func (i uint64Indexer) FromArg(b []byte, arg any) uint64 {
	uint64ToBytes(reflect.ValueOf(arg).Uint(), b)
	return 8
}

func (i uint64Indexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	uint64ToBytes(valueByOffset[uint64](o, i.offset), b)
	return 8
}

var idType = reflect.TypeOf(memdb.ID{})

var _ memdb.Indexer = idIndexer{}
var _ memdb.Indexer = idIndexer{}

func idToBytes(id memdb.ID, b []byte) {
	copy(b, id[:])
}

type idIndexer struct {
	offset uintptr
}

func (i idIndexer) Args() []memdb.ArgSerializer {
	return []memdb.ArgSerializer{i}
}

func (i idIndexer) SizeFromObject(o unsafe.Pointer) uint64 {
	return memdb.IDLength
}

func (i idIndexer) SizeFromArg(arg any) uint64 {
	return memdb.IDLength
}

func (i idIndexer) FromArg(b []byte, arg any) uint64 {
	idToBytes(reflect.ValueOf(arg).Convert(idType).Interface().(memdb.ID), b)
	return memdb.IDLength
}

func (i idIndexer) FromObject(b []byte, o unsafe.Pointer) uint64 {
	idToBytes(valueByOffset[memdb.ID](o, i.offset), b)
	return memdb.IDLength
}

func indexerForType(t reflect.Type, offset uintptr) (memdb.Indexer, error) {
	if t.ConvertibleTo(idType) {
		return idIndexer{offset: offset}, nil
	}
	if t.ConvertibleTo(timeType) {
		return timeIndexer{offset: offset}, nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return boolIndexer{offset: offset}, nil
	case reflect.String:
		return stringIndexer{offset: offset}, nil
	case reflect.Int8:
		return int8Indexer{offset: offset}, nil
	case reflect.Int16:
		return int16Indexer{offset: offset}, nil
	case reflect.Int32:
		return int32Indexer{offset: offset}, nil
	case reflect.Int64:
		return int64Indexer{offset: offset}, nil
	case reflect.Uint8:
		return uint8Indexer{offset: offset}, nil
	case reflect.Uint16:
		return uint16Indexer{offset: offset}, nil
	case reflect.Uint32:
		return uint32Indexer{offset: offset}, nil
	case reflect.Uint64:
		return uint64Indexer{offset: offset}, nil
	default:
		return nil, errors.Errorf("unsupported type: %s", t)
	}
}
