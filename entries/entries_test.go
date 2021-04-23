package entries_test

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nireo/kantadb/entries"
)

func checkEntries(t *testing.T, e1, e2 *entries.Entry) {
	t.Helper()

	if e1.Key != e2.Key {
		t.Errorf("got the wrong key. want=%q got=%q", e1.Key, e2.Key)
	}

	if e1.Value != e2.Value {
		t.Errorf("got the wrong value. want=%q got=%q", e1.Value, e2.Value)
	}
}

func TestEntryBinary(t *testing.T) {
	e := &entries.Entry{
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
		Value: "testval",
		Key:   "testkey",
	}

	// Write the binary into binary format and try parsing it
	binBytes := e.ToBinary()

	testEntry, err := entries.EntryFromBytes(binBytes)
	if err != nil {
		t.Fatalf("could not parse entry data from bytes")
	}
	checkEntries(t, testEntry, e)
}

func TestTombstone(t *testing.T) {
	e := &entries.Entry{
		Value: "\x00",
		Key:   "testkey",
	}

	binBytes := e.ToBinary()
	testEntry, err := entries.EntryFromBytes(binBytes)
	if err != nil {
		t.Fatalf("could not parse entry data from bytes")
	}

	checkEntries(t, testEntry, e)
}

func TestAppendToFileAndParse(t *testing.T) {
	e := &entries.Entry{
		Value: "testval",
		Key:   "testkey",
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

	checkEntries(t, e, testEntry)
}

func TestEntryScanner(t *testing.T) {
	e := &entries.Entry{
		Value: "testval",
		Key:   "testkey",
	}

	tmp, err := ioutil.TempFile(os.TempDir(), "testappend-")
	if err != nil {
		t.Fatalf("error creating temporary file")
	}

	// add to the file
	if err := e.AppendToFile(tmp.Name()); err != nil {
		t.Fatalf("error appending entry content to file")
	}

	// add another entry for testing
	if err := e.AppendToFile(tmp.Name()); err != nil {
		t.Fatalf("error appending entry content to file")
	}
	// create a new entry parser
	reader := entries.InitScanner(tmp, 4096)

	first, err := reader.ReadNext()
	if err != nil {
		t.Fatalf("error parsing the first value: %s", err)
	}

	second, err := reader.ReadNext()
	if err != nil {
		t.Fatalf("error parsing the second value: %s", err)
	}

	checkEntries(t, first, e)
	checkEntries(t, second, e)
}
