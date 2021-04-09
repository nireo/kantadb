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

	tree.insert1(insertNode)
	tree.size++
}

func (tree *Tree) Get(key []byte) (value []byte, ok bool) {
	node := tree.find(key)
	if node != nil {
		return node.Value, true
	}

	return nil, false
}

func (tree *Tree) find(key []byte) *Node {
	node := tree.Root
	for node != nil {
		comp := bytes.Compare(key, node.Key)
		switch {
		case comp == 0:
			return node
		case comp < 0:
			node = node.Left
		case comp < 0:
			node = node.Right
		}
	}

	return nil
}

func (tree *Tree) insert1(node *Node) {
	if node.Parent == nil {
		node.Color = black
	} else {
		tree.insert2(node)
	}
}

func (tree *Tree) insert2(node *Node) {
	if getColor(node.Parent) == black {
		return
	}

	tree.insert3(node)
}

func (tree *Tree) insert3(node *Node) {
	uncle := node.uncle()
	if getColor(uncle) == red {
		node.Parent.Color = black
		uncle.Color = black
		node.grandparent().Color = red
		tree.insert1(node.grandparent())
	} else {
		tree.insert4(node)
	}
}

func (tree *Tree) insert4(node *Node) {
	grandparent := node.grandparent()
	if node == node.Parent.Right && node.Parent == grandparent.Left {
		tree.lRotate(node.Parent)
		node = node.Left
	} else if node == node.Parent.Left && node.Parent == grandparent.Right {
		tree.rRotate(node.Parent)
		node = node.Right
	}

	tree.insert5(node)
}

func (tree *Tree) insert5(node *Node) {
	node.Parent.Color = black
	grandparent := node.grandparent()
	grandparent.Color = red

	if node == node.Parent.Left && node.Parent == grandparent.Left {
		tree.rRotate(grandparent)
	} else if node == node.Parent.Right && node.Parent == grandparent.Right {
		tree.lRotate(grandparent)
	}
}

func (node *Node) uncle() *Node {
	if node == nil || node.Parent == nil || node.Parent.Parent == nil {
		return nil
	}

	return node.Parent.sibling()
}

func (node *Node) sibling() *Node {
	if node == nil || node.Parent == nil {
		return nil
	}

	if node == node.Parent.Left {
		return node.Parent.Right
	}

	return node.Parent.Left
}

func (node *Node) grandparent() *Node {
	if node != nil && node.Parent != nil {
		return node.Parent.Parent
	}
	return nil
}

func getColor(node *Node) nodeColor {
	if node == nil {
		return black
	}

	return node.Color
}

func (tree *Tree) lRotate(node *Node) {
	right := node.Right
	tree.replace(node, right)
	node.Right = right.Left
	if right.Left != nil {
		right.Left.Parent = node
	}
	right.Left = node
	node.Parent = right
}

func (tree *Tree) rRotate(node *Node) {
	left := node.Left
	tree.replace(node, left)
	node.Left = left.Right
	if left.Right != nil {
		left.Right.Parent = node
	}
	left.Right = node
	node.Parent = left
}

func (tree *Tree) replace(old *Node, new *Node) {
	if old.Parent == nil {
		tree.Root = new
	} else {
		if old == old.Parent.Left {
			old.Parent.Left = new
		} else {
			old.Parent.Right = new
		}
	}
	if new != nil {
		new.Parent = old.Parent
	}
}
