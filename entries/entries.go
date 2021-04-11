package entries

import (
	"encoding/binary"
	"fmt"
	"os"
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

	if tombstone {
		entry.Type = Tombstone
	} else {
		entry.Type = KVPair
	}

	return entry, nil
}

// ToBinary converts the keys into the binary representation in bytes
func (e *Entry) ToBinary() []byte {
	klenBuffer := make([]byte, 4)
	vlenBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(klenBuffer, uint32(len([]byte(e.Key))))
	binary.BigEndian.PutUint32(vlenBuffer, uint32(len([]byte(e.Value))))

	data := make([]byte, 1)
	data = append(data, klenBuffer...)
	data = append(data, vlenBuffer...)
	data = append(data, []byte(e.Key)...)
	data = append(data, []byte(e.Value)...)

	return data
}

// WriteEntriesToBinary takes in a a list of entries and returns a complete byte buffer that contains
// all of the entries in the binary format.
func WriteEntriesToBinary(entries []*Entry) []byte {
	var res []byte
	for _, entry := range entries {
		res = append(res, entry.ToBinary()...)
	}

	return res
}

// AppendToFile adds the given entry to the end of a specified file
func (e *Entry) AppendToFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	if _, err := file.Write(e.ToBinary()); err != nil {
		return err
	}

	return nil
}