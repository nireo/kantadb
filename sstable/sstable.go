package sstable

import (
	"github.com/emirpasic/gods/trees/redblacktree"
)

type Config struct {
	bufferSize int
	filename   string
}

type SSTable struct {
	data *redblacktree.Tree
	conf *Config
}

func NewSSTable(config *Config) *SSTable {
	if config.bufferSize == 0 {
		config.bufferSize = 4096
	}
	s := SSTable{
		conf: config,
	}

	// implement rebuilding the index

	return &s
}
