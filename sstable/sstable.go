package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
	"github.com/nireo/kantadb/mem"
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

	// mapping the key offsets in the file
	offsets map[string]int64
	file    *os.File // the file pointer in whichi we read the entries.
}

// NewSSTable creates a sstable instance with a filename pointing to the sstable.
func NewSSTable(name string) *SSTable {
	return &SSTable{
		Filename:    name,
		BloomFilter: bloom.New(10000, 5),
		offsets:     make(map[string]int64),
	}
}

// PopulateReadOnlyFile populates the 'file' field in the sstable for faster access
func (ss *SSTable) PopulateReadOnlyFile() error {
	file, err := os.OpenFile(ss.Filename, os.O_RDONLY, 0600)
	if err != nil {
		file.Close()
		return err
	}
	ss.file = file

	return nil
}

// NewSSTableWithFilter returns a sstable with a given filename and filter
func NewSSTableWithFilter(name string, filter *bloom.BloomFilter) *SSTable {
	return &SSTable{
		Filename:    name,
		BloomFilter: filter,
	}
}

func (ss *SSTable) Close() error {
	return ss.file.Close()
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

// ReadFromOffset is an optimized read function that reads for the offset map
// the part of the sstable where the key-value pair is. This of course consumes
// more memory, but the read performance is better.
func (ss *SSTable) ReadFromOffset(key string) (string, error) {
	var value string
	offset, ok := ss.offsets[key]
	if !ok {
		return "", errors.New("key not in offset map")
	}
	// remember to set the file position
	defer ss.file.Seek(0, 0)

	if _, err := ss.file.Seek(offset, 0); err != nil {
		return "", err
	}

	return value, nil
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
func (ss *SSTable) fillTree() {
	ss.Tree = redblacktree.NewWithStringComparator()

	file, err := os.OpenFile(ss.Filename, os.O_RDONLY, 0660)
	if err != nil {
		// do nothing
		return
	}
	defer file.Close()

	offset := int64(0)
	// create a entry reader to read all values from the files
	entryScanner := entries.InitScanner(file, 4096)

	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			break
		}

		ss.offsets[entry.Key] = offset
		offset += int64(len(entry.ToBinary()))
	}
}

// ParseSSTableFromFile takes in a file path and parses that file's bloom filter file and
// it also scans all of the entries in the file and then creates the offset map.
func ParseSSTableFromFile(path string) (*SSTable, error) {
	sst := NewSSTable(path)

	// load the corresponding filter file
	if err := sst.ParseFilterFromDirectory(); err != nil {
		return nil, err
	}
	sst.PopulateReadOnlyFile()

	// build the offset index
	offset := int64(0)
	entryScanner := entries.InitScanner(sst.file, 4096)

	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			break
		}

		sst.offsets[entry.Key] = offset
		offset += int64(len(entry.ToBinary()))
	}

	return sst, nil
}

// ConstructFromMemtable takes creates a sstable with the corrent offset map from a memory table
func ConstructFromMemtable(directory string, mem *mem.MEM) (*SSTable, error) {
	timestamp := time.Now().UnixNano()
	filePath := filepath.Join(directory, fmt.Sprintf("%v.ss", timestamp))
	sst := NewSSTable(filePath)

	file, err := os.Create(sst.Filename)
	if err != nil {
		// error happened skip this and try again on the next iteration
		utils.PrintDebug("error creating sstable: %s", err)
		return nil, err
	}
	defer file.Close()

	offset := int64(0)
	for _, e := range mem.ConvertIntoEntries() {
		// a bloom filter is just a optimization to that helps finding out
		// if a value has already been seen.
		sst.BloomFilter.Add([]byte(e.Key))
		inBinary := e.ToBinary()
		file.Write(inBinary)

		// construct the parse index
		sst.offsets[e.Key] = offset
		offset += int64(len(inBinary))
	}

	if err := sst.WriteFilterToDisk(); err != nil {
		return nil, err
	}

	return sst, nil
}
