// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	volume "github.com/go-volumes/interface"
	"github.com/go-volumes/oci/registry"
)

// Options configures Freeze.
type Options struct {
	// ChunkSize is the fixed chunk window in bytes; it must be a power of two.
	// Zero selects DefaultChunkSize.
	ChunkSize int64
}

// chunkSize returns the effective, validated chunk size.
func (o Options) chunkSize() (int64, error) {
	cs := o.ChunkSize
	if cs == 0 {
		cs = DefaultChunkSize
	}
	if cs < 1 || cs&(cs-1) != 0 {
		return 0, fmt.Errorf("oci: chunk size %d must be a power of two", cs)
	}
	return cs, nil
}

// Freeze reads src in fixed-size chunk windows, pushes each UNIQUE chunk to dst
// once (deduplicating by content — all-zero holes collapse to nothing stored),
// builds a config blob carrying the geometry and ordered chunk-digest index,
// and pushes an OCI image manifest tagged ref whose layers are the deduped
// chunk blobs. It returns the manifest digest.
func Freeze(ctx context.Context, src volume.ReadOnly, dst *registry.Client, ref string, opts Options) (string, error) {
	cs, err := opts.chunkSize()
	if err != nil {
		return "", err
	}
	size, err := src.Size()
	if err != nil {
		return "", fmt.Errorf("oci: source size: %w", err)
	}
	if size < 0 {
		return "", fmt.Errorf("oci: negative source size %d", size)
	}

	zero := make([]byte, cs)
	buf := make([]byte, cs)

	nChunks := (size + cs - 1) / cs
	chunks := make([]string, 0, nChunks)
	// seen tracks unique non-zero chunk digests already pushed, so each distinct
	// blob is uploaded once and recorded once as a layer.
	seen := map[string]descriptor{}
	var layers []descriptor

	for off := int64(0); off < size; off += cs {
		n := cs
		if rem := size - off; rem < n {
			n = rem
		}
		window := buf[:n]
		if err := readFull(src, window, off); err != nil {
			return "", fmt.Errorf("oci: read chunk @%d: %w", off, err)
		}
		// All-zero hole: record an empty digest, store nothing.
		if bytes.Equal(window, zero[:n]) {
			chunks = append(chunks, "")
			continue
		}
		// Push (dedup-guarded) and index by digest.
		data := make([]byte, n)
		copy(data, window)
		digest, err := dst.PushBlob(ctx, data)
		if err != nil {
			return "", fmt.Errorf("oci: push chunk @%d: %w", off, err)
		}
		chunks = append(chunks, digest)
		if _, ok := seen[digest]; !ok {
			d := descriptor{MediaType: ChunkMediaType, Digest: digest, Size: int64(n)}
			seen[digest] = d
			layers = append(layers, d)
		}
	}

	// Config blob: geometry + ordered chunk-digest index. Marshaling a struct of
	// strings, ints and a string slice cannot fail, so the error is discarded.
	cfg := Config{MediaType: ConfigMediaType, Size: size, ChunkSize: cs, Chunks: chunks}
	cfgJSON, _ := json.Marshal(cfg)
	cfgDigest, err := dst.PushBlob(ctx, cfgJSON)
	if err != nil {
		return "", fmt.Errorf("oci: push config: %w", err)
	}

	// Manifest: custom-typed config + deduped chunk layers.
	mf := manifest{
		SchemaVersion: 2,
		MediaType:     registry.MediaTypeManifest,
		ArtifactType:  ConfigMediaType,
		Config: descriptor{
			MediaType:    ConfigMediaType,
			Digest:       cfgDigest,
			Size:         int64(len(cfgJSON)),
			ArtifactType: ConfigMediaType,
		},
		Layers: layers,
	}
	// As with the config, this concrete manifest type always marshals cleanly.
	mfJSON, _ := json.Marshal(mf)
	digest, err := dst.PutManifest(ctx, ref, registry.MediaTypeManifest, mfJSON)
	if err != nil {
		return "", fmt.Errorf("oci: put manifest: %w", err)
	}
	return digest, nil
}

// readFull fills p from r at off, treating an EOF after a short read at the tail
// as success (the final chunk may be shorter than the window).
func readFull(r io.ReaderAt, p []byte, off int64) error {
	n := 0
	for n < len(p) {
		m, err := r.ReadAt(p[n:], off+int64(n))
		n += m
		if err != nil {
			if err == io.EOF && n == len(p) {
				return nil
			}
			return err
		}
	}
	return nil
}
