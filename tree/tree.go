package tree

// New creates new tree.
func New[V any]() *Tree[V] {
	return &Tree[V]{
		revision: 1,
	}
}

// Tree is a very simple immutable binary tree to keep collection of objects.
// Objects are not stored in order. They cannot be deleted, nor iterated.
type Tree[V any] struct {
	revision        uint64
	genesisRevision uint64
	root            *node[V]
}

// Clone creates next version of the tree.
func (t *Tree[V]) Clone() *Tree[V] {
	genesisRevision := t.revision
	t.revision++
	return &Tree[V]{
		revision:        t.revision,
		genesisRevision: genesisRevision,
		root:            t.root,
	}
}

// Get gets value from the tree.
func (t *Tree[V]) Get(key uint64) (*V, bool) {
	n := t.root
	for {
		if n == nil {
			return nil, false
		}

		if n.key == key {
			return n.value, n.valueRevision > t.genesisRevision
		}

		bit := key & 0x01
		key >>= 1

		if bit > 0 {
			n = n.left
		} else {
			n = n.right
		}
	}
}

// Set sets value in the tree.
func (t *Tree[V]) Set(key uint64, value *V) {
	n := &t.root
	for {
		if *n == nil {
			*n = &node[V]{
				nodeRevision:  t.revision,
				valueRevision: t.revision,
				key:           key,
				value:         value,
			}
			return
		}

		if (*n).nodeRevision != t.revision {
			newN := **n
			newN.nodeRevision = t.revision
			*n = &newN
		}

		if (*n).key == key {
			(*n).value = value
			(*n).valueRevision = t.revision
			return
		}

		bit := key & 0x01
		key >>= 1

		if bit > 0 {
			n = &(*n).left
		} else {
			n = &(*n).right
		}
	}
}

type node[V any] struct {
	nodeRevision  uint64
	valueRevision uint64
	key           uint64
	value         *V
	left          *node[V]
	right         *node[V]
}
