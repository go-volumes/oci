// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

// Package oci freezes a block volume image into an immutable, content-addressed
// OCI artifact (Freeze, push to a registry) and re-opens it as a read-only
// block backing (OpenReadOnly, pull from a registry).
//
// A volume is sliced into fixed-size chunks; each chunk is sha256-addressed and
// pushed once. Identical chunks — most importantly the all-zero holes of a
// sparse image — collapse to a single blob, so a mostly-empty disk freezes to a
// handful of blobs regardless of its logical size. The recovered *Image serves
// reads by pulling chunks on demand (with an LRU byte cache) and rejects every
// write, so it can be handed to pool.OpenWith as a genuinely immutable backing.
package oci

import (
	"github.com/go-volumes/oci/registry"
)

// ConfigMediaType is the artifact type carried by the manifest config blob,
// marking this image as a go-volumes frozen pool image rather than a runnable
// container.
const ConfigMediaType = "application/vnd.go-volumes.pool-image.v1+json"

// ChunkMediaType is the media type of each chunk layer descriptor.
const ChunkMediaType = "application/vnd.go-volumes.pool-image.chunk.v1"

// DefaultChunkSize is the chunk window used when Options.ChunkSize is zero.
const DefaultChunkSize = 4 << 20 // 4 MiB

// Config is the JSON config blob: the image geometry plus the ordered list of
// per-chunk digests. Reading chunk i means fetching Chunks[i]; an empty string
// denotes an all-zero chunk that need not be stored at all.
type Config struct {
	// MediaType identifies the payload format.
	MediaType string `json:"mediaType"`
	// Size is the logical image size in bytes.
	Size int64 `json:"size"`
	// ChunkSize is the fixed chunk window in bytes (a power of two).
	ChunkSize int64 `json:"chunkSize"`
	// Chunks is the ordered per-chunk digest index, one entry per chunk window
	// covering [0, Size). "" means an all-zero (hole) chunk.
	Chunks []string `json:"chunks"`
}

// descriptor is an OCI content descriptor (a subset of the spec: the fields we
// emit and read back).
type descriptor struct {
	MediaType    string `json:"mediaType"`
	Digest       string `json:"digest"`
	Size         int64  `json:"size"`
	ArtifactType string `json:"artifactType,omitempty"`
}

// manifest is the OCI image manifest we push: schema 2, a custom-typed config
// blob, and the deduped set of chunk blobs as layers.
type manifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	ArtifactType  string       `json:"artifactType,omitempty"`
	Config        descriptor   `json:"config"`
	Layers        []descriptor `json:"layers"`
}

// Compile-time assurance the registry media-type constant stays in sync.
var _ = registry.MediaTypeManifest
