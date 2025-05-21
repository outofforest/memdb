package tree

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestTree(t *testing.T) {
	requireT := require.New(t)

	tree := New[int]()

	v, dirty := tree.Get(0)
	requireT.Nil(v)
	requireT.False(dirty)
	v, dirty = tree.Get(10)
	requireT.Nil(v)
	requireT.False(dirty)

	tree.Set(5, lo.ToPtr(3))
	tree.Set(0, lo.ToPtr(11))
	tree.Set(2, lo.ToPtr(4))

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

	tree.Set(5, lo.ToPtr(30))
	tree.Set(0, lo.ToPtr(110))
	tree.Set(2, lo.ToPtr(40))

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

	tree := New[uint64]()

	for i := range uint64(10_000) {
		tree.Set(i, lo.ToPtr(i+100))
	}
	for i := range uint64(10_000) {
		v, dirty := tree.Get(i)
		requireT.NotNil(v)
		requireT.Equal(i+100, *v)
		requireT.True(dirty)
	}

	for i := range uint64(20_000) {
		tree.Set(i, lo.ToPtr(i+200))
	}
	for i := range uint64(20_000) {
		v, dirty := tree.Get(i)
		requireT.NotNil(v)
		requireT.Equal(i+200, *v)
		requireT.True(dirty)
	}
}

func TestClone(t *testing.T) {
	requireT := require.New(t)

	tree1 := New[int]()
	tree1.Set(1, lo.ToPtr(10))
	tree1.Set(2, lo.ToPtr(20))
	tree1.Set(3, lo.ToPtr(30))
	tree1.Set(4, lo.ToPtr(40))
	tree1.Set(5, lo.ToPtr(50))

	tree2 := tree1.Clone()

	tree1.Set(5, lo.ToPtr(51))
	tree2.Set(5, lo.ToPtr(52))
	tree1.Set(6, lo.ToPtr(61))
	tree2.Set(6, lo.ToPtr(62))
	tree1.Set(7, lo.ToPtr(71))
	tree2.Set(8, lo.ToPtr(82))

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
	requireT.Equal(30, *v)
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

	v, dirty = tree2.Get(3)
	requireT.NotNil(v)
	requireT.Equal(30, *v)
	requireT.False(dirty)

	v, dirty = tree2.Get(4)
	requireT.NotNil(v)
	requireT.Equal(40, *v)
	requireT.False(dirty)

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
