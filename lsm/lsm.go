package lsm

import (
	"github.com/nireo/kantadb/sstable"
	"github.com/nireo/kantadb/tree"
)

type LSM struct {
	Alive    bool
	Memtable *tree.Tree
	SSTables []*sstable.SSTable
}

func NewLSM() *LSM {
	return &LSM{
		Alive:    false,
		Memtable: tree.NewTree(),
		SSTables: make([]*sstable.SSTable, 0),
	}
}

func (l *LSM) Run() {
	l.Alive = true
}
