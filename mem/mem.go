package mem

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
	"github.com/nireo/kantadb/utils"
)

// I think this is the most simple way to represent in-memory values instead of a tree,
// since this is probably more performant and over all more idiomatic. We can also add
// new methods if needed.

var logPath string
var logFileWriteMutex = &sync.Mutex{}

// MEM represents the in-memory data
type MEM struct {
	size        int64
	tree        *redblacktree.Tree
	logFilePath string
	logFile     *os.File
}

// SetLogPath sets the folder in which all of the log are to be stored.
// The log directory is the same as the sstable directory.
func SetLogPath(path string) {
	logPath = path
}

// Put adds a value to the data
func (m *MEM) Put(key, val string) error {
	if err := m.AppendToLog(key, val); err != nil {
		return err
	}

	m.tree.Put(key, val)
	m.addToSize(key, val)

	return nil
}

func (m *MEM) addToSize(key, val string) {
	m.size += int64(9 + len([]byte(key)) + len([]byte(val)))
}

// Get finds a value in the table and returns a status on if the item is found.
func (m *MEM) Get(key string) (string, bool) {
	val, ok := m.tree.Get(key)
	if !ok {
		return "", ok
	}

	return val.(string), ok
}

// Size returns the amount of elements in the table
func (m *MEM) Size() int64 {
	return m.size
}

// New creates a new instance of a memory table
func New() *MEM {
	timestamp := time.Now().UnixNano()
	filePath := filepath.Join(logPath, fmt.Sprintf("%v.lg", timestamp))

	// create the new file
	file, err := os.Create(filePath)
	if err != nil {
		// error happened skip this and try again on the next iteration
		utils.PrintDebug("error creating logfile: %s", err)
	}
	defer file.Close()

	utils.PrintDebug("created a log file at: %s", filePath)
	mem := &MEM{
		tree:        redblacktree.NewWithStringComparator(),
		logFilePath: filePath,
		size:        0,
	}

	mem.PopulateLogFile()
	return mem
}

// PopulateLogFile opens/creates the log file, and it handles settings the
// logFile file-pointer to the file with the logFilePath
func (m *MEM) PopulateLogFile() error {
	f, err := os.OpenFile(m.logFilePath, os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		f.Close()
		return err
	}
	m.logFile = f

	return nil
}

// CreateTableFromLog constructs a memory table from the contents of a log file.
// This is used to check if some memtables were left in-memory when closing.
func CreateTableFromLog(logFilePath string) (*MEM, error) {
	file, err := os.Open(logFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	table := &MEM{
		tree:        redblacktree.NewWithStringComparator(),
		logFilePath: logFilePath,
		size:        0,
	}

	entryScanner := entries.InitScanner(file, 4096)
	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			// we couldn't parse a entry file so the file has ended
			break
		}

		table.tree.Put(entry.Key, entry.Value)
		table.size += int64(9 + len(entry.Key) + len(entry.Value))
	}

	// now that the log file is useless we can just delete it, since the table is going into
	// the memory table queue.
	if err := os.Remove(logFilePath); err != nil {
		return nil, err
	}

	return table, nil
}

// ConvertIntoEntries converts the database key-value pairs into entries which are
// then used to write values to the disk
func (m *MEM) ConvertIntoEntries() []*entries.Entry {
	var entrs []*entries.Entry
	iter := m.tree.Iterator()
	for iter.Next() {
		entrs = append(entrs, &entries.Entry{
			Key:   iter.Key().(string),
			Value: iter.Value().(string),
		})
	}

	return entrs
}

// AppendToLog takes in a key-value pair and appends that to the file pointer.
func (m *MEM) AppendToLog(key, value string) error {
	logFileWriteMutex.Lock()
	defer logFileWriteMutex.Unlock()

	toWrite := entries.KeyValueToBytes(key, value)
	_, err := m.logFile.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

// Remove removes the log file after the memtable is written into persistent
// storage.
func (m *MEM) Remove() error {
	logFileWriteMutex.Lock()
	defer logFileWriteMutex.Unlock()
	err := m.logFile.Close()
	if err != nil {
		return err
	}

	if err := os.Remove(m.logFilePath); err != nil {
		return err
	}

	return nil
}

// DeleteLogFile deletes the log file after all of the values have been stored into an sstable
func (m *MEM) DeleteLogFile() error {
	return os.Remove(m.logFilePath)
}

// GetTree returns the tree of the table. This is used for copying the data for flushing
func (m *MEM) GetTree() *redblacktree.Tree {
	return m.tree
}
