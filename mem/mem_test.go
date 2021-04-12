package mem_test

import (
	"testing"

	"github.com/nireo/kantadb/entries"
	"github.com/nireo/kantadb/mem"
)

func TestSimpleOperations(t *testing.T) {
	m := mem.New()

	m.Put("hello", "world")
	val, ok := m.Get("hello")
	if !ok {
		t.Errorf("could not find the hello value")
	}

	if val != "world" {
		t.Errorf("the value is wrong")
	}

	if m.Size() != 1 {
		t.Errorf("the is wrong")
	}
}

func TestConvertToEntries(t *testing.T) {
	m := mem.New()

	expectedKeyOrder := []string{"1", "2", "3"}

	m.Put("2", "val2")
	m.Put("1", "val1")
	m.Put("3", "val3")

	entrs := m.ConvertIntoEntries()

	for index, entry := range entrs {
		if entry.Key != expectedKeyOrder[index] {
			t.Errorf("keys are not in expected order")
		}

		if entry.Type != entries.KVPair {
			t.Errorf("entry is not of type KVPair")
		}
	}
}
