package sstable

import (
	"github.com/emirpasic/gods/trees/redblacktree"
)

type SSTable struct {
	Tree     *redblacktree.Tree
	Filename string // the file in which the sstable is stored
}

func NewSSTable(name string) *SSTable {
	return &SSTable{
		Tree:     redblacktree.NewWithStringComparator(),
		Filename: name,
	}
}
