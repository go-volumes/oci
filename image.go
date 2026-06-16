// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	volume "github.com/go-volumes/interface"
	"github.com/go-volumes/oci/registry"
)

// ErrReadOnly is returned by every mutating method of an *Image: it is a frozen,
// content-addressed artifact and cannot be written through.
var ErrReadOnly = errors.New("oci: read-only frozen image")

// defaultCacheChunks is the number of decoded chunks the LRU cache holds.
const defaultCacheChunks = 16

// Image is a read-only block backing recovered from a frozen OCI artifact. It
// serves ReadAt by pulling the covering chunk blobs on demand (an LRU byte
// cache avoids re-pulling hot chunks); hole chunks read as zeros. It satisfies
// both volume.ReadOnly and pool.Backing, and rejects all writes with
// ErrReadOnly so pool.OpenWith gets a genuinely immutable store.
type Image struct {
	src       *registry.Client
	size      int64
	chunkSize int64
	chunks    []string // per-chunk digest; "" == hole

	mu      sync.Mutex
	cache   *lru
	zeroBuf []byte // shared all-zero window for hole chunks (lazy)
}

var _ volume.ReadOnly = (*Image)(nil)

// OpenReadOnly pulls and parses the manifest tagged ref, recovers the config
// (size, chunk size, ordered chunk-digest index) and returns an *Image serving
// reads from src.
func OpenReadOnly(ctx context.Context, src *registry.Client, ref string) (*Image, error) {
	_, mfBody, err := src.GetManifest(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("oci: get manifest: %w", err)
	}
	var mf manifest
	if err := json.Unmarshal(mfBody, &mf); err != nil {
		return nil, fmt.Errorf("oci: parse manifest: %w", err)
	}
	if mf.Config.Digest == "" {
		return nil, fmt.Errorf("oci: manifest has no config descriptor")
	}
	cfgBody, err := src.PullBlob(ctx, mf.Config.Digest)
	if err != nil {
		return nil, fmt.Errorf("oci: pull config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(cfgBody, &cfg); err != nil {
		return nil, fmt.Errorf("oci: parse config: %w", err)
	}
	if cfg.Size < 0 {
		return nil, fmt.Errorf("oci: config has negative size %d", cfg.Size)
	}
	if cfg.ChunkSize < 1 || cfg.ChunkSize&(cfg.ChunkSize-1) != 0 {
		return nil, fmt.Errorf("oci: config chunk size %d not a power of two", cfg.ChunkSize)
	}
	wantChunks := (cfg.Size + cfg.ChunkSize - 1) / cfg.ChunkSize
	if int64(len(cfg.Chunks)) != wantChunks {
		return nil, fmt.Errorf("oci: config has %d chunk digests, want %d for size %d / chunk %d",
			len(cfg.Chunks), wantChunks, cfg.Size, cfg.ChunkSize)
	}
	return &Image{
		src:       src,
		size:      cfg.Size,
		chunkSize: cfg.ChunkSize,
		chunks:    cfg.Chunks,
		cache:     newLRU(defaultCacheChunks),
	}, nil
}

// Size reports the logical image size in bytes.
func (im *Image) Size() (int64, error) { return im.size, nil }

// Close releases the image. The underlying registry client is the caller's.
func (im *Image) Close() error { return nil }

// ReadAt assembles len(p) bytes at off from the covering chunks, pulling each by
// digest (cached) and treating hole chunks as zeros. A read past the end returns
// the bytes available and io.EOF, matching io.ReaderAt semantics.
func (im *Image) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("oci: negative offset %d", off)
	}
	cs := im.chunkSize
	n := 0
	for n < len(p) {
		pos := off + int64(n)
		if pos >= im.size {
			return n, io.EOF
		}
		ci := pos / cs
		within := pos % cs
		cnt := int(cs - within)
		if cnt > len(p)-n {
			cnt = len(p) - n
		}
		// Clamp the tail chunk to the logical size.
		if end := ci*cs + int64(within) + int64(cnt); end > im.size {
			cnt = int(im.size - (ci*cs + within))
		}
		chunk, err := im.chunk(ci)
		if err != nil {
			return n, err
		}
		copy(p[n:n+cnt], chunk[within:within+int64(cnt)])
		n += cnt
	}
	return n, nil
}

// chunk returns the decoded bytes of chunk ci (a full cs-sized slice, the tail
// chunk zero-padded to cs), pulling and caching it as needed. A hole digest ("")
// yields a shared all-zero slice.
func (im *Image) chunk(ci int64) ([]byte, error) {
	digest := im.chunks[ci]
	if digest == "" {
		// Hole: a cs-sized zero window. Callers only read it, never mutate.
		return im.zeroChunk(), nil
	}
	im.mu.Lock()
	if b, ok := im.cache.get(digest); ok {
		im.mu.Unlock()
		return b, nil
	}
	im.mu.Unlock()

	data, err := im.src.PullBlob(context.Background(), digest)
	if err != nil {
		return nil, fmt.Errorf("oci: pull chunk %s: %w", digest, err)
	}
	// Normalise to a full cs window so ReadAt indexing is uniform; the stored
	// blob may be shorter (the tail chunk).
	buf := data
	if int64(len(buf)) < im.chunkSize {
		padded := make([]byte, im.chunkSize)
		copy(padded, buf)
		buf = padded
	}
	im.mu.Lock()
	im.cache.put(digest, buf)
	im.mu.Unlock()
	return buf, nil
}

// zeroChunk returns the lazily-built shared all-zero chunk window used for hole
// chunks. Callers only read it.
func (im *Image) zeroChunk() []byte {
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.zeroBuf == nil {
		im.zeroBuf = make([]byte, im.chunkSize)
	}
	return im.zeroBuf
}
