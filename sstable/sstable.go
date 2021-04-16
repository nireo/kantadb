package sstable

import (
	"os"
	"strings"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
	"github.com/willf/bloom"
)

// SSTable represents a sorted list of key-value pairs stored in a given file.
type SSTable struct {
	// TODO: use the tree for sparse index
	Tree        *redblacktree.Tree
	Filename    string // the file in which the sstable is stored
	BloomFilter *bloom.BloomFilter
}

// NewSSTable creates a sstable instance with a filename pointing to the sstable.
func NewSSTable(name string) *SSTable {
	return &SSTable{
		Filename:    name,
		BloomFilter: bloom.New(20000, 5),
	}
}

// Get finds a key from the list of sstable files.
func (ss *SSTable) Get(key string) (string, bool) {
	if !ss.BloomFilter.Test([]byte(key)) {
		return "", false
	}

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

// AppendToFile adds the given entry to the end of a table and adds that to the bloom filter
func (ss *SSTable) AppendToTable(e *entries.Entry) error {
	file, err := os.OpenFile(ss.Filename, os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}
	defer file.Close()

	ss.BloomFilter.Add([]byte(e.Key))

	if _, err := file.Write(e.ToBinary()); err != nil {
		return err
	}

	return nil
}

// WriteFilterToDisk takes the bloom filter and places it into disk
func (ss *SSTable) WriteFilterToDisk() error {
	withoutSuffix := strings.TrimSuffix(ss.Filename, ".ss")
	file, err := os.Create(withoutSuffix + ".fltr")
	if err != nil {
		return err
	}

	defer file.Close()

	if _, err := ss.BloomFilter.WriteTo(file); err != nil {
		return err
	}

	return nil
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
