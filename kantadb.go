package kantadb

import (
	"github.com/nireo/kantadb/mem"
	"github.com/nireo/kantadb/sstable"
)

type DB struct {
	Alive    bool
	MEM      *mem.MEM
	SSTables []*sstable.SSTable

	// configuration
	maxMEMsize int
	ssdir      string
}

func NewLSM(storageDir string) *DB {
	return &DB{
		Alive:    false,
		MEM:      mem.New(),
		SSTables: make([]*sstable.SSTable, 0),
		ssdir:    storageDir,
	}
}

func (db *DB) Run() {
	db.Alive = true
}

func (db *DB) Get(key string) (val string, ok bool) {
	val, ok = db.MEM.Get(val)
	if !ok {
		// search from sstables
	}
	return
}

func (db *DB) Put(key, val string) {
	if db.MEM.Size() > db.maxMEMsize {
		// flush the tree
	}

	db.MEM.Put(key, val)
}
