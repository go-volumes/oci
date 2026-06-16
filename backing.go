// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

// The methods below complete the pool.Backing shape (ReadAt + Size + Close come
// from image.go) while making the image immutable: every mutating call returns
// ErrReadOnly. This lets a frozen artifact be handed to pool.OpenWith for
// read-only access without any risk of mutation reaching the registry.
//
// A compile-time `var _ pool.Backing = (*Image)(nil)` assertion lives in the
// test package (importing pool is test-only, per the module's dependency rule).

// WriteAt always fails: a frozen image is read-only.
func (im *Image) WriteAt(p []byte, off int64) (int, error) { return 0, ErrReadOnly }

// Truncate always fails: a frozen image is read-only.
func (im *Image) Truncate(size int64) error { return ErrReadOnly }

// Sync always fails: a frozen image is read-only, so any attempt to commit
// buffered state is rejected. A read-only consumer that never mutates never
// calls Sync.
func (im *Image) Sync() error { return ErrReadOnly }
