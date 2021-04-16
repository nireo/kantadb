package sstable

import (
	"os"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
)

// SSTable represents a sorted list of key-value pairs stored in a given file.
type SSTable struct {
	// TODO: use the tree for sparse index
	Tree     *redblacktree.Tree
	Filename string // the file in which the sstable is stored
}

// NewSSTable creates a sstable instance with a filename pointing to the sstable.
func NewSSTable(name string) *SSTable {
	return &SSTable{
		Filename: name,
	}
}

// Get finds a key from the list of sstable files.
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

// fillTree creates a sparse index to speed up look up time.
// REFER: https://en.wikipedia.org/wiki/Database_index
// TODO: create a function that utilizes the sparse index
func (ss *SSTable) fillTree() {
	ss.Tree = redblacktree.NewWithStringComparator()

	file, err := os.OpenFile(ss.Filename, os.O_RDONLY, 0660)
	if err != nil {
		// do nothing
		return
	}
	defer file.Close()

	var (
		curOffset  int
		prevOffset int
	)

	// create a entry reader to read all values from the files
	entryScanner := entries.InitScanner(file, 4096)

	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			break
		}

		if ss.Tree.Empty() || curOffset-prevOffset > 4096 {
			ss.Tree.Put(entry.Key, curOffset)
			prevOffset = curOffset
		}

		curOffset += len(entry.ToBinary())
	}
}
