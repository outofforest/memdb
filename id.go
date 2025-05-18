package memdb

import (
	"crypto/rand"

	"github.com/samber/lo"
)

// IDLength specifies the length of ID field.
const IDLength = 16

// ID is used to define ID field in entities.
type ID [IDLength]byte

type idConstraint interface {
	~[IDLength]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	var id [IDLength]byte
	lo.Must(rand.Read(id[:]))
	return id
}
