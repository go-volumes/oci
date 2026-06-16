// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

import (
	"fmt"
	"io"
	"sync"

	volume "github.com/go-volumes/interface"
)

// Overlay is a read-write block backing layered over an immutable OCI base
// [Image]. Writes are buffered in memory — the base is never mutated — and reads
// merge the dirty overlay over the base (a never-written region falls through to
// the base, or reads zero past it). [Overlay.Sync] is a no-op: durability is the
// explicit [Overlay.Commit], which snapshots the current state into a new
// versioned OCI artifact and pushes blobs ONLY for the chunks that changed.
//
// Because the overlay absorbs every write — including a pool's own metadata
// writes — pool.OpenWith(overlay) yields a fully read-write pool over a frozen,
// content-addressed base: edit, then Commit to a new tag.
type Overlay struct {
	base      *Image
	chunkSize int64

	mu        sync.Mutex
	size      int64
	dirty     map[int64][]byte // chunk index -> full chunkSize buffer
	baseLimit int64            // chunks [0,baseLimit) still fall through to the base
}

var _ volume.Device = (*Overlay)(nil)

// NewOverlay wraps an immutable base [Image] in a writable overlay, starting at
// the base's size and chunk size.
func NewOverlay(base *Image) *Overlay {
	return &Overlay{
		base:      base,
		chunkSize: base.chunkSize,
		size:      base.size,
		dirty:     map[int64][]byte{},
		baseLimit: int64(len(base.chunks)),
	}
}

// Size reports the current logical size, which may exceed the base after a grow.
func (o *Overlay) Size() (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.size, nil
}

// Sync is a no-op: the overlay is in memory and durability is provided by
// [Overlay.Commit].
func (o *Overlay) Sync() error { return nil }

// Close releases the overlay; the base [Image] and its registry client remain
// the caller's to close.
func (o *Overlay) Close() error { return nil }

// readChunk returns the bytes of chunk ci for reading: the dirty buffer if the
// chunk has been written, else the base's chunk (or a shared zero window past
// the base). The returned slice must not be mutated by the caller.
func (o *Overlay) readChunk(ci int64) ([]byte, error) {
	if b, ok := o.dirty[ci]; ok {
		return b, nil
	}
	if ci < o.baseLimit {
		return o.base.chunk(ci)
	}
	return o.base.zeroChunk(), nil
}

// ReadAt assembles len(p) bytes at off, merging the dirty overlay over the base.
// A read past the current size returns the available bytes and io.EOF.
func (o *Overlay) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("oci: negative offset %d", off)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	cs := o.chunkSize
	n := 0
	for n < len(p) {
		pos := off + int64(n)
		if pos >= o.size {
			return n, io.EOF
		}
		ci := pos / cs
		within := pos % cs
		cnt := int(cs - within)
		if cnt > len(p)-n {
			cnt = len(p) - n
		}
		if end := ci*cs + within + int64(cnt); end > o.size {
			cnt = int(o.size - (ci*cs + within))
		}
		chunk, err := o.readChunk(ci)
		if err != nil {
			return n, err
		}
		copy(p[n:n+cnt], chunk[within:within+int64(cnt)])
		n += cnt
	}
	return n, nil
}

// writeChunk returns a writable dirty buffer for chunk ci, materialising it from
// the base on first write so the chunk's unwritten bytes are preserved.
func (o *Overlay) writeChunk(ci int64) ([]byte, error) {
	if b, ok := o.dirty[ci]; ok {
		return b, nil
	}
	buf := make([]byte, o.chunkSize)
	if ci < o.baseLimit {
		src, err := o.base.chunk(ci)
		if err != nil {
			return nil, err
		}
		copy(buf, src)
	}
	o.dirty[ci] = buf
	return buf, nil
}

// WriteAt writes p at off into the in-memory overlay (read-modify-write per
// chunk), growing the logical size when it writes past the current end.
func (o *Overlay) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("oci: negative offset %d", off)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	cs := o.chunkSize
	n := 0
	for n < len(p) {
		pos := off + int64(n)
		ci := pos / cs
		within := pos % cs
		cnt := int(cs - within)
		if cnt > len(p)-n {
			cnt = len(p) - n
		}
		buf, err := o.writeChunk(ci)
		if err != nil {
			return n, err
		}
		copy(buf[within:within+int64(cnt)], p[n:n+cnt])
		n += cnt
	}
	if end := off + int64(len(p)); end > o.size {
		o.size = end
	}
	return n, nil
}

// Truncate sets the logical size, dropping dirty chunks wholly beyond it on a
// shrink (base chunks past the size become invisible too). A grow is sparse.
func (o *Overlay) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("oci: negative truncate size %d", size)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if size < o.size {
		keep := (size + o.chunkSize - 1) / o.chunkSize // chunks needed to cover [0,size)
		for ci := range o.dirty {
			if ci >= keep {
				delete(o.dirty, ci)
			}
		}
		// Mask base chunks beyond the shrink so a later grow reads zeros (POSIX
		// truncate semantics) rather than re-exposing the immutable base.
		if keep < o.baseLimit {
			o.baseLimit = keep
		}
	}
	o.size = size
	return nil
}
