package mem

import (
	"encoding/binary"
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
}

// SetLogPath sets the folder in which all of the log are to be stored.
// The log directory is the same as the sstable directory.
func SetLogPath(path string) {
	logPath = path
}

// Put adds a value to the data
func (m *MEM) Put(key, val string) {
	m.WriteToLog(key, val)

	m.tree.Put(key, val)
	m.addToSize(key, val)
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
func (m *MEM) Size() int {
	return int(m.size)
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
	return &MEM{
		tree:        redblacktree.NewWithStringComparator(),
		logFilePath: filePath,
		size:        0,
	}
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
	}

	entryScanner := entries.InitScanner(file, 4096)
	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			// we couldn't parse a entry file so the file has ended
			break
		}

		table.tree.Put(entry.Key, entry.Value)
	}

	utils.PrintDebug("created a table from log file of size: %d", table.Size())

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
			Type:  entries.KVPair,
		})
	}

	// no need to sort them since they're already sorted

	return entrs
}

// WriteToLog appends a key-value pair into the log
func (m *MEM) WriteToLog(key, val string) {
	logFileWriteMutex.Lock()

	entry := entries.Entry{
		Key:   key,
		Value: val,
		Type:  entries.KVPair,
	}

	entry.AppendToFile(m.logFilePath)

	logFileWriteMutex.Unlock()
}

// DeleteLogFile deletes the log file after all of the values have been stored into an sstable
func (m *MEM) DeleteLogFile() error {
	return os.Remove(m.logFilePath)
}

// MaxMEM contains a binary tree with a max capacity
type MaxMEM struct {
	Tree    *redblacktree.Tree
	Size    int64
	wrMutex sync.RWMutex
	lgMutex sync.Mutex
	lgFile  *os.File
}

// NewMaxMEM creates a memtable with a max capacity
func NewMaxMEM() *MaxMEM {
	timestamp := time.Now().UnixNano()
	filePath := filepath.Join(logPath, fmt.Sprintf("%v.lg", timestamp))

	// create the new file
	file, err := os.Create(filePath)
	if err != nil {
		// error happened skip this and try again on the next iteration
		utils.PrintDebug("error creating logfile: %s", err)
	}

	return &MaxMEM{
		Tree:    redblacktree.NewWithStringComparator(),
		Size:    0,
		lgFile:  file,
		lgMutex: sync.Mutex{},
		wrMutex: sync.RWMutex{},
	}
}

// Put places a key into the in-memory tree and then updates the size of the tree.
func (mx *MaxMEM) Put(key, val string) {
	mx.wrMutex.Lock()
	defer mx.wrMutex.Unlock()

	mx.Tree.Put(key, val)
	klenBuffer := make([]byte, 4)
	vlenBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(klenBuffer, uint32(len([]byte(key))))
	binary.BigEndian.PutUint32(vlenBuffer, uint32(len([]byte(val))))

	data := make([]byte, 1)
	data = append(data, klenBuffer...)
	data = append(data, vlenBuffer...)
	data = append(data, []byte(key)...)
	data = append(data, []byte(val)...)

	mx.Size += int64(len(data))

	mx.lgFile.Write(data)
}

// Get finds a value from the in-memory table
func (mx *MaxMEM) Get(key string) (string, bool) {
	mx.wrMutex.RLock()
	val, ok := mx.Tree.Get("")
	mx.wrMutex.RUnlock()

	return fmt.Sprintf("%v", val), ok
}

// Close closes the log file connection
func (mx *MaxMEM) Close() error {
	return mx.lgFile.Close()
}
