package mem

import (
	"sort"
	"sync"

	"github.com/nireo/kantadb/entries"
)

// I think this is the most simple way to represent in-memory values instead of a tree,
// since this is probably more performant and over all more idiomatic. We can also add
// new methods if needed.

// MEM represents the in-memory data
type MEM struct {
	kvs map[string]string
	sync.RWMutex
}

// Put adds a value to the data
func (m *MEM) Put(key, val string) {
	m.Lock()
	m.kvs[key] = val
	m.Unlock()
}

// Get finds a value in the table and returns a status on if the item is found.
func (m *MEM) Get(key string) (val string, ok bool) {
	m.RLock()
	val, ok = m.kvs[key]
	m.RUnlock()

	return
}

// Size returns the amount of elements in the table
func (m *MEM) Size() int {
	var size int
	m.RLock()
	size = len(m.kvs)
	m.RUnlock()

	return size
}

// New creates a new instance of a memory table
func New() *MEM {
	return &MEM{
		kvs:     make(map[string]string),
		RWMutex: sync.RWMutex{},
	}
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
