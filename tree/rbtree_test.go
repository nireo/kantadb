package tree_test

import (
	"bytes"
	"testing"

	"github.com/nireo/kantadb/tree"
)

func TestInsertGet(t *testing.T) {
	key := []byte("testkey")
	val := []byte("testval")

	tree := tree.NewTree()
	tree.Insert(key, val)

	value, ok := tree.Get(key)
	if !ok {
		t.Fatalf("error getting key: %q from rbtree", string(value))
	}

	if !bytes.Equal(value, val) {
		t.Fatalf("the values are not equal")
	}

	if tree.Size() != 1 {
		t.Fatalf("tree size is wrong. got=%d want=1", tree.Size())
	}
}

func TestUpdate(t *testing.T) {
	key := []byte("testkey")
	val := []byte("testval1")
	newVal := []byte("testval2")

	tree := tree.NewTree()
	tree.Insert(key, val)
	value, ok := tree.Get(key)
	if !ok {
		t.Fatalf("error getting key: %q from rbtree", string(value))
	}

	if !bytes.Equal(value, val) {
		t.Fatalf("the values are not equal")
	}

	tree.Insert(key, newVal)
	value, ok = tree.Get(key)
	if !ok {
		t.Fatalf("error getting key: %q from rbtree", string(value))
	}

	if !bytes.Equal(value, newVal) {
		t.Fatalf("the value was not updated. got=%q, want=%q", string(value), string(newVal))
	}

	if tree.Size() != 1 {
		t.Fatalf("tree size is wrong. got=%d want=1", tree.Size())
	}
}

func TestDelete(t *testing.T) {
	key := []byte("testkey")
	val := []byte("testval1")

	tree := tree.NewTree()
	tree.Insert(key, val)

	tree.Remove(key)

	// now the key should be not found
	if _, ok := tree.Get(key); ok {
		t.Fatalf("could still get key-value after deletion")
	}

	if tree.Size() != 0 {
		t.Fatalf("tree size is wrong. got=%d want=0", tree.Size())
	}
}
