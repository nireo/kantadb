package sstable

import (
	"os"

	"github.com/emirpasic/gods/trees/redblacktree"
)

type SSTable struct {
	// TODO: use the tree for sparse index
	Tree     *redblacktree.Tree
	Filename string // the file in which the sstable is stored
}

func NewSSTable(name string) *SSTable {
	return &SSTable{
		Tree:     redblacktree.NewWithStringComparator(),
		Filename: name,
	}
}

func (ss *SSTable) FillTree() {
	f, err := os.OpenFile(ss.Filename, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()
}
