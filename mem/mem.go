package mem

import (
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

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
	kvs         map[string]string
	logFilePath string
}

func SetLogPath(path string) {
	logPath = path
}

// Put adds a value to the data
func (m *MEM) Put(key, val string) {
	m.WriteToLog(key, val)
	m.kvs[key] = val
}

// Get finds a value in the table and returns a status on if the item is found.
func (m *MEM) Get(key string) (val string, ok bool) {
	val, ok = m.kvs[key]

	return
}

// Size returns the amount of elements in the table
func (m *MEM) Size() int {
	var size int
	size = len(m.kvs)

	return size
}

// New creates a new instance of a memory table
func New(logFilePath string) *MEM {
	// create a random identifier
	rand.Seed(time.Now().UnixNano())

	_, err := os.Create(filepath.Join(
		logPath, logFilePath,
	))
	if err != nil {
		utils.PrintDebug("could not create log file: %s", err)
	}

	return &MEM{
		kvs:         make(map[string]string),
		logFilePath: logFilePath,
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
		kvs:         make(map[string]string),
		logFilePath: logFilePath,
	}

	entryScanner := entries.InitScanner(file, 4096)
	for {
		entry, err := entryScanner.ReadNext()
		if err != nil {
			// we couldn't parse a entry file so the file has ended
			break
		}

		table.kvs[entry.Key] = entry.Value
	}

	utils.PrintDebug("created a table from log file of size: %d", table.Size())

	// now that the log file is useless we can just delete it, since the table is going into
	// the memory table queue.
	if err := os.Remove(logFilePath); err != nil {
		return nil, err
	}

	return table, nil
}

// ConvertIntoEntires converts the database key-value pairs into entries which are
// then used to write values to the disk
func (m *MEM) ConvertIntoEntries() []*entries.Entry {
	var entrs []*entries.Entry
	for key, value := range m.kvs {
		entrs = append(entrs, &entries.Entry{
			Key:   key,
			Value: value,
			Type:  entries.KVPair,
		})
	}

	// sort them
	sort.Slice(entrs, func(i, j int) bool {
		return entrs[i].Key < entrs[j].Key
	})

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

	entry.AppendToFile(filepath.Join(logPath, m.logFilePath))

	logFileWriteMutex.Unlock()
}

// DeleteLogFile deletes the log file after all of the values have been stored into an sstable
func (m *MEM) DeleteLogFile() error {
	return os.Remove(filepath.Join(logPath, m.logFilePath))
}
