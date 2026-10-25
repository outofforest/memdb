package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTree(t *testing.T) {
	requireT := require.New(t)

	tree := New[*int]()

	v, dirty := tree.Get(0)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(10)
	requireT.Nil(v)
	requireT.False(dirty)

	tree.Set(5, new(3))
	tree.Set(0, new(11))
	tree.Set(2, new(4))

	v, dirty = tree.Get(10)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(3)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(11)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(4)
	requireT.Nil(v)
	requireT.False(dirty)

	v, dirty = tree.Get(5)
	requireT.NotNil(v)
	requireT.Equal(3, *v)
	requireT.True(dirty)

	v, dirty = tree.Get(0)
	requireT.NotNil(v)
	requireT.Equal(11, *v)
	requireT.True(dirty)

	v, dirty = tree.Get(2)
	requireT.NotNil(v)
	requireT.Equal(4, *v)
	requireT.True(dirty)

	tree.Set(5, new(30))
	tree.Set(0, new(110))
	tree.Set(2, new(40))

	v, dirty = tree.Get(10)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(3)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(11)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(4)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(30)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(110)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(40)
	requireT.Nil(v)
	requireT.False(dirty)

	v, dirty = tree.Get(5)
	requireT.NotNil(v)
	requireT.Equal(30, *v)
	requireT.True(dirty)

	v, dirty = tree.Get(0)
	requireT.NotNil(v)
	requireT.Equal(110, *v)
	requireT.True(dirty)

	v, dirty = tree.Get(2)
	requireT.NotNil(v)
	requireT.Equal(40, *v)
	requireT.True(dirty)
}

func TestTreeBig(t *testing.T) {
	requireT := require.New(t)

	tree := New[*uint64]()

	for i := range uint64(10_000) {
		tree.Set(i, new(i+100))
	}
	for i := range uint64(10_000) {
		v, dirty := tree.Get(i)
		requireT.NotNil(v)
		requireT.Equal(i+100, *v)
		requireT.True(dirty)
	}

	for i := range uint64(20_000) {
		tree.Set(i, new(i+200))
	}
	for i := range uint64(20_000) {
		v, dirty := tree.Get(i)
		requireT.NotNil(v)
		requireT.Equal(i+200, *v)
		requireT.True(dirty)
	}
}

func TestNext(t *testing.T) {
	requireT := require.New(t)

	tree1 := New[*int]()
	tree1.Set(1, new(10))
	tree1.Set(2, new(20))
	tree1.Set(3, new(30))
	tree1.Set(4, new(40))
	tree1.Set(5, new(50))

	tree2 := tree1.Next()

	tree1.Set(3, new(31))
	tree2.Set(4, new(42))
	tree1.Set(5, new(51))
	tree2.Set(5, new(52))
	tree1.Set(6, new(61))
	tree2.Set(6, new(62))
	tree1.Set(7, new(71))
	tree2.Set(8, new(82))

	v, dirty := tree1.Get(1)
	requireT.NotNil(v)
	requireT.Equal(10, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(2)
	requireT.NotNil(v)
	requireT.Equal(20, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(3)
	requireT.NotNil(v)
	requireT.Equal(31, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(4)
	requireT.NotNil(v)
	requireT.Equal(40, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(5)
	requireT.NotNil(v)
	requireT.Equal(51, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(6)
	requireT.NotNil(v)
	requireT.Equal(61, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(7)
	requireT.NotNil(v)
	requireT.Equal(71, *v)
	requireT.True(dirty)

	v, dirty = tree1.Get(8)
	requireT.Nil(v)
	requireT.False(dirty)

	v, dirty = tree2.Get(1)
	requireT.NotNil(v)
	requireT.Equal(10, *v)
	requireT.False(dirty)

	v, dirty = tree2.Get(2)
	requireT.NotNil(v)
	requireT.Equal(20, *v)
	requireT.False(dirty)

	// This test case describes that this mechanism is not cloning.
	v, dirty = tree2.Get(3)
	requireT.NotNil(v)
	requireT.Equal(31, *v)
	requireT.False(dirty)

	v, dirty = tree2.Get(4)
	requireT.NotNil(v)
	requireT.Equal(42, *v)
	requireT.True(dirty)

	v, dirty = tree2.Get(5)
	requireT.NotNil(v)
	requireT.Equal(52, *v)
	requireT.True(dirty)

	v, dirty = tree2.Get(6)
	requireT.NotNil(v)
	requireT.Equal(62, *v)
	requireT.True(dirty)

	v, dirty = tree2.Get(7)
	requireT.Nil(v)
	requireT.False(dirty)

	v, dirty = tree2.Get(8)
	requireT.NotNil(v)
	requireT.Equal(82, *v)
	requireT.True(dirty)
}
