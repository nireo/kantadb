package main

import "bytes"

type nodeColor bool

const (
	black nodeColor = true
	red   nodeColor = false
)

type Tree struct {
	Root *Node
	size int
}

type Node struct {
	Key   []byte
	Value []byte

	Color  nodeColor
	Left   *Node
	Right  *Node
	Parent *Node
}

func NewTree() *Tree {
	return &Tree{}
}

func (tree *Tree) Insert(key, val []byte) {
	var insertNode *Node
	if tree.Root == nil {
		tree.Root = &Node{Key: key, Value: val, Color: red}
		insertNode = tree.Root
	} else {
		node := tree.Root
		while := true
		for while {
			compare := bytes.Compare(key, node.Key)
			switch {
			case compare == 0:
				node.Key = key
				node.Value = val
				return
			case compare < 0:
				if node.Left == nil {
					node.Left = &Node{Key: key, Value: val, Color: red}
					insertNode = node.Left
					while = false
				} else {
					node = node.Left
				}
			case compare > 0:
				if node.Right == nil {
					node.Right = &Node{Key: key, Value: val, Color: red}
					insertNode = node.Right
					while = false
				} else {
					node = node.Right
				}
			}
		}
		insertNode.Parent = node
	}
	tree.size++
}
