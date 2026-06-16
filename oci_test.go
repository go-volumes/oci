// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	volume "github.com/go-volumes/interface"
	"github.com/go-volumes/oci"
	"github.com/go-volumes/oci/registry"
	"github.com/go-volumes/pool"
)

// Compile-time proof the recovered image plugs into BOTH go-volumes contracts:
// the read-only block device and the pool's physical backing.
var (
	_ volume.ReadOnly = (*oci.Image)(nil)
	_ pool.Backing    = (*oci.Image)(nil)
)

// memBacking is an in-memory pool.Backing for tests (no temp files needed).
type memBacking struct {
	mu  sync.Mutex
	buf []byte
}

func (m *memBacking) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off >= int64(len(m.buf)) {
		return 0, io.EOF
	}
	n := copy(p, m.buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memBacking) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	end := off + int64(len(p))
	if end > int64(len(m.buf)) {
		m.buf = append(m.buf, make([]byte, end-int64(len(m.buf)))...)
	}
	copy(m.buf[off:end], p)
	return len(p), nil
}

func (m *memBacking) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if size <= int64(len(m.buf)) {
		m.buf = m.buf[:size]
	} else {
		m.buf = append(m.buf, make([]byte, size-int64(len(m.buf)))...)
	}
	return nil
}

func (m *memBacking) Sync() error  { return nil }
func (m *memBacking) Close() error { return nil }

// memReadOnly is a trivial volume.ReadOnly over a byte slice, used to freeze a
// hand-built image directly (without going through a pool).
type memReadOnly struct{ buf []byte }

func (m *memReadOnly) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.buf)) {
		return 0, io.EOF
	}
	n := copy(p, m.buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (m *memReadOnly) Size() (int64, error) { return int64(len(m.buf)), nil }
func (m *memReadOnly) Close() error         { return nil }

// ---- a counting in-memory registry --------------------------------------

type fakeReg struct {
	mu        sync.Mutex
	blobs     map[string][]byte
	manifests map[string][]byte
	mtypes    map[string]string
	uploads   int
	pushCount int // successful blob PUTs (the dedup metric)
}

func newReg() *fakeReg {
	return &fakeReg{
		blobs:     map[string][]byte{},
		manifests: map[string][]byte{},
		mtypes:    map[string]string{},
	}
}

func (f *fakeReg) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.Contains(p, "/blobs/uploads/"):
			f.mu.Lock()
			f.uploads++
			id := f.uploads
			f.mu.Unlock()
			w.Header().Set("Location", fmt.Sprintf("/v2/repo/uploads-put/%d", id))
			w.WriteHeader(http.StatusAccepted)
		case strings.Contains(p, "/uploads-put/"):
			digest := r.URL.Query().Get("digest")
			body, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.blobs[digest] = body
			f.pushCount++
			f.mu.Unlock()
			w.Header().Set("Docker-Content-Digest", digest)
			w.WriteHeader(http.StatusCreated)
		case strings.Contains(p, "/blobs/"):
			digest := p[strings.LastIndex(p, "/")+1:]
			f.mu.Lock()
			b, ok := f.blobs[digest]
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"errors":[{"code":"BLOB_UNKNOWN"}]}`)
				return
			}
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write(b)
		case strings.Contains(p, "/manifests/"):
			ref := p[strings.LastIndex(p, "/")+1:]
			if r.Method == http.MethodPut {
				body, _ := io.ReadAll(r.Body)
				d := registry.Digest(body)
				f.mu.Lock()
				f.manifests[ref] = body
				f.mtypes[ref] = r.Header.Get("Content-Type")
				f.manifests[d] = body
				f.mtypes[d] = r.Header.Get("Content-Type")
				f.mu.Unlock()
				w.Header().Set("Docker-Content-Digest", d)
				w.WriteHeader(http.StatusCreated)
				return
			}
			f.mu.Lock()
			body, ok := f.manifests[ref]
			mt := f.mtypes[ref]
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`)
				return
			}
			w.Header().Set("Content-Type", mt)
			w.Write(body)
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

func startReg(t *testing.T, f *fakeReg) (*registry.Client, func()) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	return &registry.Client{BaseURL: srv.URL, Repository: "repo"}, srv.Close
}

// TestPoolRoundTripAndDedup is the headline test: build a pool volume with a
// recognizable pattern and large sparse holes, ExportRaw it, Freeze it to a
// registry, OpenReadOnly, mount it under a SECOND pool via OpenWith, and verify
// the bytes (and that holes read zero). It also asserts content dedup: a mostly
// sparse, repetitive image pushes far fewer blobs than it has chunks.
func TestPoolRoundTripAndDedup(t *testing.T) {
	ctx := context.Background()
	const blockSize = 4096
	const chunk = 64 * 1024 // 64 KiB chunks
	const volSize = 4 << 20 // 4 MiB logical

	// --- source pool ---
	src := &memBacking{}
	p, err := pool.CreateWith(src, volSize, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	v, err := p.CreateVolume("root", volSize)
	if err != nil {
		t.Fatal(err)
	}

	// Recognizable pattern near the start and near the end; everything else is a
	// hole (reads zero). Two identical 'A' regions one chunk apart let us prove
	// identical NON-zero chunks also dedup.
	patA := bytes.Repeat([]byte{0xA5}, chunk)
	if _, err := v.WriteAt(patA, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := v.WriteAt(patA, 2*chunk); err != nil { // identical chunk again
		t.Fatal(err)
	}
	tail := []byte("FROZEN-TAIL-MARKER")
	if _, err := v.WriteAt(tail, volSize-int64(len(tail))); err != nil {
		t.Fatal(err)
	}

	// Export the volume to a flat raw image we can freeze as a ReadOnly.
	raw := &memBacking{}
	if err := raw.Truncate(volSize); err != nil {
		t.Fatal(err)
	}
	if err := v.ExportRaw(raw); err != nil {
		t.Fatal(err)
	}
	srcRO := &memReadOnly{buf: raw.buf}

	// --- freeze ---
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()

	mfDigest, err := oci.Freeze(ctx, srcRO, client, "v1", oci.Options{ChunkSize: chunk})
	if err != nil {
		t.Fatal(err)
	}
	if mfDigest == "" {
		t.Fatal("empty manifest digest")
	}

	totalChunks := volSize / chunk
	t.Logf("chunks=%d blobsPushed=%d manifest=%s", totalChunks, reg.pushCount, mfDigest)
	// One 'A' blob (deduped across two occurrences) + one tail blob + one config
	// blob = 3 pushed blobs, versus 64 chunks. Assert a large reduction and that
	// the zero hole was never pushed as a chunk.
	if reg.pushCount >= int(totalChunks) {
		t.Fatalf("dedup failed: pushed %d blobs for %d chunks", reg.pushCount, totalChunks)
	}
	if reg.pushCount != 3 {
		t.Fatalf("expected 3 pushed blobs (A + tail + config), got %d", reg.pushCount)
	}

	// --- open read-only and verify the manifest/config shape ---
	img, err := oci.OpenReadOnly(ctx, client, "v1")
	if err != nil {
		t.Fatal(err)
	}
	defer img.Close()
	if sz, _ := img.Size(); sz != volSize {
		t.Fatalf("image size %d != %d", sz, volSize)
	}

	// --- mount the frozen image under a fresh pool via OpenWith ---
	// The image IS a Backing, so write a pool header into a COPY first: instead,
	// we verify direct ReadAt round-trip here (the pool-as-consumer path is
	// exercised in TestImageAsPoolBacking below with a frozen pool image).
	got := make([]byte, chunk)
	if _, err := img.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, patA) {
		t.Fatal("region A mismatch after round-trip")
	}
	got2 := make([]byte, chunk)
	if _, err := img.ReadAt(got2, 2*chunk); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, patA) {
		t.Fatal("second region A mismatch")
	}
	// A hole reads zero.
	hole := make([]byte, chunk)
	if _, err := img.ReadAt(hole, chunk); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hole, make([]byte, chunk)) {
		t.Fatal("hole did not read zero")
	}
	// The tail marker survived.
	tg := make([]byte, len(tail))
	if _, err := img.ReadAt(tg, volSize-int64(len(tail))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tg, tail) {
		t.Fatalf("tail marker mismatch: %q", tg)
	}
}

// TestImageAsPoolBacking freezes a *pool image* (a pool laid out on a backing),
// re-opens it, and runs a second pool.OpenWith on the frozen image — the real
// consumer path. Reads must succeed; writes must be rejected with ErrReadOnly.
func TestImageAsPoolBacking(t *testing.T) {
	ctx := context.Background()
	const blockSize = 4096
	const chunk = 64 * 1024
	const cap = 2 << 20

	// Build a pool ON a memBacking, create a volume, write data, close so the
	// metadata is flushed to the backing.
	mb := &memBacking{}
	p, err := pool.CreateWith(mb, cap, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	v, err := p.CreateVolume("data", 256*1024)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("POOL"), 1024) // 4 KiB
	if _, err := v.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil { // flush header + metadata into mb
		t.Fatal(err)
	}

	// Freeze the whole backing image (header + data + metadata) byte-for-byte.
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	srcRO := &memReadOnly{buf: append([]byte(nil), mb.buf...)}
	if _, err := oci.Freeze(ctx, srcRO, client, "pool", oci.Options{ChunkSize: chunk}); err != nil {
		t.Fatal(err)
	}

	// Re-open as a frozen image and mount a SECOND pool on it read-only.
	img, err := oci.OpenReadOnly(ctx, client, "pool")
	if err != nil {
		t.Fatal(err)
	}
	defer img.Close()

	p2, err := pool.OpenWith(img)
	if err != nil {
		t.Fatalf("pool.OpenWith(frozen image): %v", err)
	}
	v2, err := p2.OpenVolume("data")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := v2.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("frozen pool image read-back mismatch")
	}

	// Writes through the frozen backing are impossible.
	if _, err := img.WriteAt([]byte("x"), 0); !errors.Is(err, oci.ErrReadOnly) {
		t.Fatalf("WriteAt: expected ErrReadOnly, got %v", err)
	}
	if err := img.Truncate(10); !errors.Is(err, oci.ErrReadOnly) {
		t.Fatalf("Truncate: expected ErrReadOnly, got %v", err)
	}
	if err := img.Sync(); !errors.Is(err, oci.ErrReadOnly) {
		t.Fatalf("Sync: expected ErrReadOnly, got %v", err)
	}
}

// TestFreezeDefaultChunkSize covers the zero-ChunkSize default path.
func TestFreezeDefaultChunkSize(t *testing.T) {
	ctx := context.Background()
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	src := &memReadOnly{buf: bytes.Repeat([]byte{1}, 8192)}
	if _, err := oci.Freeze(ctx, src, client, "d", oci.Options{}); err != nil {
		t.Fatal(err)
	}
	img, err := oci.OpenReadOnly(ctx, client, "d")
	if err != nil {
		t.Fatal(err)
	}
	if sz, _ := img.Size(); sz != 8192 {
		t.Fatalf("size %d", sz)
	}
}

// TestFreezeEmptySource freezes a zero-length source: no chunks, just a config.
func TestFreezeEmptySource(t *testing.T) {
	ctx := context.Background()
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	if _, err := oci.Freeze(ctx, &memReadOnly{}, client, "empty", oci.Options{ChunkSize: 4096}); err != nil {
		t.Fatal(err)
	}
	if reg.pushCount != 1 { // only the config blob
		t.Fatalf("empty source pushed %d blobs, want 1 (config)", reg.pushCount)
	}
	img, err := oci.OpenReadOnly(ctx, client, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if sz, _ := img.Size(); sz != 0 {
		t.Fatalf("size %d", sz)
	}
	// Reading any byte returns EOF immediately.
	if _, err := img.ReadAt(make([]byte, 1), 0); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF on empty image, got %v", err)
	}
}
