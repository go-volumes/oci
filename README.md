# go-volumes/oci

Freeze a block-volume image into an immutable, content-addressed **OCI artifact**
(push to a registry) and re-open it as a read-only block backing (pull from a
registry). Pure Go, `CGO_ENABLED=0`, standard library only (plus the
`github.com/go-volumes/interface` contract).

A volume is sliced into fixed-size chunks; each chunk is sha256-addressed and
pushed **once**. Identical chunks — most importantly the all-zero holes of a
sparse image — collapse to a single blob, so a mostly-empty disk freezes to a
handful of blobs regardless of its logical size. The recovered image serves
reads by pulling chunks on demand (LRU byte cache) and rejects every write, so
it plugs straight into `github.com/go-volumes/pool`'s `OpenWith` as a genuinely
immutable backing.

## Packages

- **`registry`** — a minimal, dependency-free OCI Distribution v2 client over
  `net/http`: monolithic blob upload, HEAD-based dedup, manifest get/put, and
  anonymous / HTTP Basic / Docker–OCI bearer-token auth. sha256 digests; every
  pulled blob is verified.
- **`oci`** (root) — `Freeze` and `OpenReadOnly`.

## Use

```go
// Freeze a read-only volume into a registry as an OCI artifact.
mfDigest, err := oci.Freeze(ctx, src /* volume.ReadOnly */, client, "myimage:v1",
    oci.Options{ChunkSize: 4 << 20})

// Re-open it as a read-only block backing and mount a pool on it.
img, err := oci.OpenReadOnly(ctx, client, "myimage:v1")
p, err := pool.OpenWith(img) // img is volume.ReadOnly AND pool.Backing
```

## Artifact shape

- **Manifest** (`application/vnd.oci.image.manifest.v1+json`): `artifactType` and
  `config.mediaType` are `application/vnd.go-volumes.pool-image.v1+json`; `layers`
  are the **deduped** set of chunk-blob descriptors
  (`application/vnd.go-volumes.pool-image.chunk.v1`).
- **Config** blob (JSON):

  ```json
  {"mediaType":"application/vnd.go-volumes.pool-image.v1+json",
   "size":N,"chunkSize":C,"chunks":["sha256:…","",...]}
  ```

  `chunks` is the ordered per-chunk digest index; `""` marks an all-zero hole
  that is never stored as a blob.

## License

BSD-3-Clause — see [LICENSE](LICENSE).
