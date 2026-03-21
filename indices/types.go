package indices

import "github.com/outofforest/memdb"

// Index defines the interface of index.
type Index[T any] interface {
	memdb.Index
	dummyTDefiner(t T)
}
