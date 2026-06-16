// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

import "container/list"

// lru is a fixed-capacity, digest-keyed byte cache of decoded chunks. It is not
// safe for concurrent use; the Image guards it with its mutex.
type lru struct {
	cap   int
	ll    *list.List // front = most recently used
	items map[string]*list.Element
}

// entry is one cached chunk.
type entry struct {
	key  string
	data []byte
}

// newLRU returns an LRU holding at most capacity chunks (minimum 1).
func newLRU(capacity int) *lru {
	if capacity < 1 {
		capacity = 1
	}
	return &lru{cap: capacity, ll: list.New(), items: map[string]*list.Element{}}
}

// get returns the cached bytes for key and marks it most-recently-used.
func (c *lru) get(key string) ([]byte, bool) {
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*entry).data, true
}

// put inserts or refreshes key, evicting the least-recently-used entry when the
// cache is over capacity.
func (c *lru) put(key string, data []byte) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*entry).data = data
		return
	}
	el := c.ll.PushFront(&entry{key: key, data: data})
	c.items[key] = el
	if c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.items, oldest.Value.(*entry).key)
		}
	}
}
