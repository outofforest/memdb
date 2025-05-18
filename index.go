package memdb

import "reflect"

// Index defines the interface of index.
type Index interface {
	ID() uint64
	Type() reflect.Type
	NumOfArgs() uint64
	Schema() *IndexSchema
}
