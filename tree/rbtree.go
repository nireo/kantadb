package tree

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

func (tree *Tree) Put(key, value []byte) {
	var insertedNode *Node
	if tree.Root == nil {
		tree.Root = &Node{Key: key, Value: value, Color: red}
		insertedNode = tree.Root
	} else {
		node := tree.Root
		loop := true
		for loop {
			compare := bytes.Compare(key, node.Key)
			switch {
			case compare == 0:
				node.Key = key
				node.Value = value
				return
			case compare < 0:
				if node.Left == nil {
					node.Left = &Node{Key: key, Value: value, Color: red}
					insertedNode = node.Left
					loop = false
				} else {
					node = node.Left
				}
			case compare > 0:
				if node.Right == nil {
					node.Right = &Node{Key: key, Value: value, Color: red}
					insertedNode = node.Right
					loop = false
				} else {
					node = node.Right
				}
			}
		}
		insertedNode.Parent = node
	}

	tree.insert1(insertedNode)
	tree.size++
}

func (tree *Tree) Remove(key []byte) {
	var child *Node
	node := tree.find(key)
	if node == nil {
		return
	}

	if node.Left != nil && node.Right != nil {
		mx := node.Left.max()
		node.Key = mx.Key
		node.Value = mx.Value

		node = mx
	}

	if node.Left == nil || node.Right == nil {
		if node.Right == nil {
			child = node.Left
		} else {
			child = node.Right
		}

		if node.Color == black {
			node.Color = getColor(child)
			tree.delete1(node)
		}

		tree.replace(node, child)
		if node.Parent == nil && child != nil {
			child.Color = black
		}
	}

	tree.size--
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

func (tree *Tree) delete1(node *Node) {
	if node.Parent == nil {
		return
	}

	tree.delete2(node)
}

func (tree *Tree) delete2(node *Node) {
	sibling := node.sibling()
	if getColor(sibling) == red {
		node.Parent.Color = red
		sibling.Color = black
		if node == node.Parent.Left {
			tree.lRotate(node.Parent)
		} else {
			tree.rRotate(node.Parent)
		}
	}

	tree.delete3(node)
}

func (tree *Tree) delete3(node *Node) {
	sibling := node.sibling()
	if getColor(node.Parent) == black &&
		getColor(sibling) == black &&
		getColor(sibling.Left) == black &&
		getColor(sibling.Right) == black {
		sibling.Color = red

		tree.delete1(node.Parent)
	} else {
		tree.delete4(node)
	}
}

func (tree *Tree) delete4(node *Node) {
	sibling := node.sibling()
	if getColor(node.Parent) == red &&
		getColor(sibling) == black &&
		getColor(sibling.Left) == black &&
		getColor(sibling.Right) == black {
		sibling.Color = red
		node.Parent.Color = black
	} else {
		tree.delete5(node)
	}
}

func (tree *Tree) delete5(node *Node) {
	sibling := node.sibling()
	if node == node.Parent.Left &&
		getColor(sibling) == black &&
		getColor(sibling.Left) == red &&
		getColor(sibling.Right) == black {
		sibling.Color = red
		sibling.Left.Color = black
		tree.rRotate(sibling)
	} else if node == node.Parent.Right &&
		getColor(sibling) == black &&
		getColor(sibling.Right) == red &&
		getColor(sibling.Left) == black {
		sibling.Color = red
		sibling.Right.Color = black
		tree.lRotate(sibling)
	}
	tree.deleteCase6(node)
}

func (tree *Tree) deleteCase6(node *Node) {
	sibling := node.sibling()
	sibling.Color = getColor(node.Parent)
	node.Parent.Color = black
	if node == node.Parent.Left && getColor(sibling.Right) == red {
		sibling.Right.Color = black
		tree.lRotate(node.Parent)
	} else if getColor(sibling.Left) == red {
		sibling.Left.Color = black
		tree.rRotate(node.Parent)
	}
}

func (node *Node) max() *Node {
	if node == nil {
		return nil
	}

	for node.Right != nil {
		node = node.Right
	}

	return node
}

func (tree *Tree) Size() int {
	return tree.size
}
