package entries

import (
	"encoding/binary"
	"fmt"
)

type EntryType byte

const (
	KVPair EntryType = iota
	Tombstone
)

// Entry represents a database entry that is written into disk
type Entry struct {
	Key   string
	Value string
	Type  EntryType
}

// EntryFromBytes takes in a some byte data and tries to encode a database entry from that data.
// The byte encoding is inspired by bitcask in the sense that the encoding includes the key length
// followed by the key and then the value length followed by the value. Also if the value starts off
// with a null-byte the entry is considered a tombstone entry.
func EntryFromBytes(bytes []byte) (*Entry, error) {
	// the bytes dont have the encoded values
	if len(bytes) < 9 {
		return nil, fmt.Errorf("data is too short. got=%d", len(bytes))
	}

	klen := binary.BigEndian.Uint32(bytes[1:5])
	vlen := binary.BigEndian.Uint32(bytes[5:9])

	if uint32(9+klen+vlen) > uint32(len(bytes)) {
		return nil, fmt.Errorf("the key and value lengths are invalid.")
	}

	// check if tombstone value
	val := string(bytes[9+klen : 9+klen+vlen])
	tombstone := val[0] == '\x00'

	entry := &Entry{
		Value: val,
		Key:   string(bytes[9 : 9+klen]),
	}

	switch tombstone {
	case true:
		entry.Type = Tombstone
	case false:
		entry.Type = KVPair
	}

	return nil, nil
}
