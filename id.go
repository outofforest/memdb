package memdb

import (
	"crypto/rand"

	"github.com/samber/lo"

	"github.com/outofforest/memdb/id"
)

// ID is used to define ID field in entities.
type ID = id.ID

type idConstraint interface {
	~[id.Length]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	var id [id.Length]byte
	lo.Must(rand.Read(id[:]))
	return id
}
