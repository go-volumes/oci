// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

import (
	"bytes"
	"testing"
)

func TestLRUUnit(t *testing.T) {
	// capacity < 1 is clamped to 1.
	c := newLRU(0)

	c.put("a", []byte("A"))
	if got, ok := c.get("a"); !ok || !bytes.Equal(got, []byte("A")) {
		t.Fatalf("get a: %q ok=%v", got, ok)
	}
	// Re-putting an existing key refreshes its value (and moves it to front).
	c.put("a", []byte("AA"))
	if got, ok := c.get("a"); !ok || !bytes.Equal(got, []byte("AA")) {
		t.Fatalf("refresh a: %q ok=%v", got, ok)
	}
	// Inserting a second key with capacity 1 evicts "a".
	c.put("b", []byte("B"))
	if _, ok := c.get("a"); ok {
		t.Fatal("expected a evicted")
	}
	if _, ok := c.get("b"); !ok {
		t.Fatal("expected b present")
	}
	// Missing key.
	if _, ok := c.get("z"); ok {
		t.Fatal("unexpected hit for z")
	}
}
