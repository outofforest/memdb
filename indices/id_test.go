package indices

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/id"
)

type entity struct {
	ID memdb.ID
}

func TestEntityIDIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	v := &entity{}

	indexer := id.Indexer{}

	v.ID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	verify(requireT, indexer, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, v, v.ID)
}
