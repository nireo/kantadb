package entries

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// EntryType specifies which type of value the entry is, for example: a tombstone
// or a key-value pair.
type EntryType byte

const (
	// KVPair represents a key-value pair
	KVPair EntryType = iota
	// Tombstone represents a deleted value.
	Tombstone

	// TombstoneValue is the value given to deleted key-value pairs.
	TombstoneValue string = "\x00"
)

// Entry represents a database entry that is written into disk
type Entry struct {
	Key   string
	Value string
	Type  EntryType
}

// EntryScanner contains the logic and settings for parsing database entries from files.
type EntryScanner struct {
	// this is embedded since this allows cleaner code
	*bufio.Scanner
}

// EntryFromBytes takes in a some byte data and tries to encode a database entry from that data.
// The byte encoding is as followed: each entry is seperated by a 0-byte; then the following 8
// bytes contain the lengths of the key and value. Then the following entries are of the specified
// length.
func EntryFromBytes(bytes []byte) (*Entry, error) {
	// the bytes dont have the encoded values
	// the first byte is empty which is why we need 9 bytes overall for the beginning
	if len(bytes) < 9 {
		return nil, fmt.Errorf("data is too short. got=%d", len(bytes))
	}

	klen := binary.BigEndian.Uint32(bytes[1:5])
	vlen := binary.BigEndian.Uint32(bytes[5:9])

	if uint32(9+klen+vlen) > uint32(len(bytes)) {
		return nil, errors.New("the key and value lengths are invalid")
	}

	// check if tombstone value
	val := string(bytes[9+klen : 9+klen+vlen])
	tombstone := val == TombstoneValue

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
	defer file.Close()

	if _, err := file.Write(e.ToBinary()); err != nil {
		return err
	}

	return nil
}

// ReadNext finds the next entry and returns the error and bytes
func (e *EntryScanner) ReadNext() (*Entry, error) {
	e.Scanner.Scan()

	return EntryFromBytes(e.Scanner.Bytes())
}

// ReadAll scans until a error is found meaning that all entries have been read.
func (e *EntryScanner) ReadAll() []*Entry {
	var entries []*Entry

	for e.Scan() {
		entry, err := EntryFromBytes(e.Bytes())
		if err != nil {
			return entries
		}

		entries = append(entries, entry)
	}

	return entries
}

// InitScanner reads the file while splitting the data using a custom bufio split function.
func InitScanner(file *os.File, readSize int) *EntryScanner {
	s := bufio.NewScanner(file)

	buffer := make([]byte, readSize)
	s.Buffer(buffer, bufio.MaxScanTokenSize)
	s.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		entry, err := EntryFromBytes(data)
		if err == nil {
			return len(entry.ToBinary()), data[:len(entry.ToBinary())], nil
		}
		return 0, nil, nil
	})

	return &EntryScanner{s}
}
