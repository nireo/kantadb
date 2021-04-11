package entries_test

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nireo/kantadb/entries"
)

func TestEntryBinary(t *testing.T) {
	e := &entries.Entry{
		Type:  entries.KVPair,
		Value: "testval",
		Key:   "testkey",
	}

	// replicate the writing process to test if equal
	klenBuffer := make([]byte, 4)
	vlenBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(klenBuffer, uint32(len([]byte(e.Key))))
	binary.BigEndian.PutUint32(vlenBuffer, uint32(len([]byte(e.Value))))

	data := make([]byte, 1)
	data = append(data, klenBuffer...)
	data = append(data, vlenBuffer...)
	data = append(data, []byte(e.Key)...)
	data = append(data, []byte(e.Value)...)

	if !bytes.Equal(e.ToBinary(), data) {
		t.Fatalf("bytes are not same")
	}
}

func TestBytesParse(t *testing.T) {
	e := &entries.Entry{
		Type:  entries.KVPair,
		Value: "testval",
		Key:   "testkey",
	}

	// Write the binary into binary format and try parsing it
	binBytes := e.ToBinary()

	testEntry, err := entries.EntryFromBytes(binBytes)
	if err != nil {
		t.Fatalf("could not parse entry data from bytes")
	}

	if testEntry.Key != e.Key {
		t.Errorf("got the wrong key. want=%q got=%q", e.Key, testEntry.Key)
	}

	if testEntry.Key != e.Key {
		t.Errorf("got the wrong key. want=%q got=%q", e.Key, testEntry.Key)
	}

	if testEntry.Value != e.Value {
		t.Errorf("got the wrong value. want=%q got=%q", e.Value, testEntry.Value)
	}
}

func TestTombstone(t *testing.T) {
	e := &entries.Entry{
		Type:  entries.Tombstone,
		Value: "testval",
		Key:   "\x00",
	}

	binBytes := e.ToBinary()
	testEntry, err := entries.EntryFromBytes(binBytes)
	if err != nil {
		t.Fatalf("could not parse entry data from bytes")
	}

	if testEntry.Key != e.Key {
		t.Errorf("got the wrong key. want=%q got=%q", e.Key, testEntry.Key)
	}

}

func TestAppendToFileAndParse(t *testing.T) {
	e := &entries.Entry{
		Type:  entries.Tombstone,
		Value: "testval",
		Key:   "\x00",
	}

	tmp, err := ioutil.TempFile(os.TempDir(), "testappend-")
	if err != nil {
		t.Fatalf("error creating temporary file")
	}

	// write to the file
	if err := e.AppendToFile(tmp.Name()); err != nil {
		t.Fatalf("error appending entry content to file")
	}

	// read the contents of the file and parse them
	data, err := ioutil.ReadFile(tmp.Name())
	if err != nil {
		t.Fatalf("error reading temp file data")
	}

	testEntry, err := entries.EntryFromBytes(data)
	if err != nil {
		t.Fatalf("error parsing content from temp file")
	}

	if testEntry.Key != e.Key {
		t.Errorf("got the wrong key. want=%q got=%q", e.Key, testEntry.Key)
	}

	if testEntry.Key != e.Key {
		t.Errorf("got the wrong key. want=%q got=%q", e.Key, testEntry.Key)
	}

	if testEntry.Value != e.Value {
		t.Errorf("got the wrong value. want=%q got=%q", e.Value, testEntry.Value)
	}
}
