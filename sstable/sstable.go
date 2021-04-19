package sstable

import (
	"os"
	"strings"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
	"github.com/nireo/kantadb/utils"
	"github.com/willf/bloom"
)

// If the buffer exceeds this size it is writen to disk
const MaxTableSize = 1 << 20

// SSTable represents a sorted list of key-value pairs stored in a given file.
type SSTable struct {
	Tree        *redblacktree.Tree // Mappings to the key offsets in the file.
	Filename    string             // the file in which the sstable is stored
	BloomFilter *bloom.BloomFilter
}

// NewSSTable creates a sstable instance with a filename pointing to the sstable.
func NewSSTable(name string) *SSTable {
	return &SSTable{
		Filename:    name,
		BloomFilter: bloom.New(20000, 5),
	}
}

// NewSSTableWithFilter returns a sstable with a given filename and filter
func NewSSTableWithFilter(name string, filter *bloom.BloomFilter) *SSTable {
	return &SSTable{
		Filename:    name,
		BloomFilter: filter,
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

// GetFilterFilename removes the .ss suffix and returns the same file with the .fltr name.
func (ss *SSTable) GetFilterFilename() string {
	withoutSuffix := strings.TrimSuffix(ss.Filename, ".ss")
	return withoutSuffix + ".fltr"
}

// ParseFilterFromTable reads a filter file containing the data for a bloom filter and then
// builds the sstables bloom filter from it.
func (ss *SSTable) ParseFilterFromDirectory() error {
	file, err := os.Open(ss.GetFilterFilename())
	if err != nil {
		return err
	}
	defer file.Close()

	// just load the bloom filter from the file
	if _, err := ss.BloomFilter.ReadFrom(file); err != nil {
		return err
	}

	return nil
}

// WriteFilterToDisk takes the bloom filter and places it into disk
func (ss *SSTable) WriteFilterToDisk() error {
	file, err := os.Create(ss.GetFilterFilename())
	if err != nil {
		return err
	}

	defer file.Close()

	if _, err := ss.BloomFilter.WriteTo(file); err != nil {
		return err
	}

	utils.PrintDebug("created a new filter file at: %s", ss.GetFilterFilename())

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
