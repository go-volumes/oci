// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-volumes/oci/registry"
)

// Commit snapshots the overlay's current state into a NEW immutable OCI artifact
// tagged ref on dst, pushing blobs ONLY for the chunks that changed since the
// base: an unchanged chunk reuses the base's digest verbatim — no pull, no
// re-hash, no re-push — so a commit costs the size of the delta, and unchanged
// chunks are shared by digest across every tag. It returns the new manifest
// digest. The overlay and its base are left unchanged; reopen ref with
// [OpenReadOnly] (e.g. to branch a new overlay from it).
//
// dst may be the base's registry (versioned tags in one repo) or a different
// one (push the snapshot elsewhere); when it is the base's registry the
// unchanged chunk blobs are already present, so only the delta uploads.
func (o *Overlay) Commit(ctx context.Context, dst *registry.Client, ref string) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	cs := o.chunkSize
	size := o.size
	nChunks := (size + cs - 1) / cs
	chunks := make([]string, nChunks)

	seen := map[string]struct{}{}
	var layers []descriptor
	addLayer := func(digest string, blobLen int64) {
		if digest == "" {
			return
		}
		if _, ok := seen[digest]; !ok {
			seen[digest] = struct{}{}
			layers = append(layers, descriptor{MediaType: ChunkMediaType, Digest: digest, Size: blobLen})
		}
	}

	zero := make([]byte, cs)
	for ci := int64(0); ci < nChunks; ci++ {
		if dirty, ok := o.dirty[ci]; ok {
			// Changed chunk: its blob length is the current (possibly tail) length.
			n := cs
			if rem := size - ci*cs; rem < n {
				n = rem
			}
			window := dirty[:n]
			if bytes.Equal(window, zero[:n]) {
				chunks[ci] = "" // all-zero now: a hole, stored as nothing
				continue
			}
			data := make([]byte, n)
			copy(data, window)
			digest, err := dst.PushBlob(ctx, data)
			if err != nil {
				return "", fmt.Errorf("oci: push chunk %d: %w", ci, err)
			}
			chunks[ci] = digest
			addLayer(digest, n)
			continue
		}
		// Unchanged chunk: reuse the base digest. Its blob length is the BASE's
		// chunk length (which may differ from this overlay's tail after a grow);
		// past the base it is a hole.
		var digest string
		var blobLen int64
		if ci < o.baseLimit {
			digest = o.base.chunks[ci]
			blobLen = cs
			if rem := o.base.size - ci*cs; rem < blobLen {
				blobLen = rem
			}
		}
		chunks[ci] = digest
		addLayer(digest, blobLen)
	}

	// Config blob: geometry + updated chunk-digest index. A struct of ints,
	// strings and a string slice always marshals cleanly.
	cfg := Config{MediaType: ConfigMediaType, Size: size, ChunkSize: cs, Chunks: chunks}
	cfgJSON, _ := json.Marshal(cfg)
	cfgDigest, err := dst.PushBlob(ctx, cfgJSON)
	if err != nil {
		return "", fmt.Errorf("oci: push config: %w", err)
	}
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
	mfJSON, _ := json.Marshal(mf)
	digest, err := dst.PutManifest(ctx, ref, registry.MediaTypeManifest, mfJSON)
	if err != nil {
		return "", fmt.Errorf("oci: put manifest: %w", err)
	}
	return digest, nil
}
