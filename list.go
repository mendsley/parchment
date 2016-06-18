package main

import (
	"github.com/mendsley/parchment/binfmt"
)

type Chain struct {
	Head *binfmt.Log
	tail *binfmt.Log
}

func (c *Chain) Append(entry *binfmt.Log) {
	if c.Head == nil {
		c.Head = entry
	} else {
		c.tail.Next = entry
	}

	for entry.Next != nil {
		entry = entry.Next
	}
	c.tail = entry
}
