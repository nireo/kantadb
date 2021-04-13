package sstable

import (
	"os"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
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

func (ss *SSTable) Get(key string) (string, bool) {
	// open the file
	file, err := os.OpenFile(ss.Filename, os.O_RDONLY, 0660)
	if err != nil {
		return "", false
	}
	defer file.Close()

	// create a entry reader to read all values from the files
	entryScanner := entries.InitScanner(file, 4096)

	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			break
		}

		if entry.Key == key {
			return entry.Key, true
		}
	}

	return "", false
}
