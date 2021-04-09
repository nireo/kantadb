package sstable

import "github.com/nireo/kantadb/tree"

type Config struct {
	bufferSize int
	filename   string
}

type SSTable struct {
	data *tree.Tree
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
