// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-volumes/oci"
	"github.com/go-volumes/oci/registry"
	"github.com/go-volumes/pool"
)

// var _ pool.Backing assures the overlay plugs in as a pool's read-write backing.
var _ pool.Backing = (*oci.Overlay)(nil)

const ovChunk = 64 * 1024 // 64 KiB chunks for the overlay tests

// freezeBase freezes buf as "v1" to a fresh counting registry and returns the
// reopened base image plus the registry and client (so a Commit can push the
// delta into the same repo and the test can read reg.pushCount).
func freezeBase(t *testing.T, buf []byte) (*oci.Image, *fakeReg, *registry.Client) {
	t.Helper()
	ctx := context.Background()
	reg := newReg()
	client, closeFn := startReg(t, reg)
	t.Cleanup(closeFn)
	if _, err := oci.Freeze(ctx, roBytes{b: buf}, client, "v1", oci.Options{ChunkSize: ovChunk}); err != nil {
		t.Fatal(err)
	}
	img, err := oci.OpenReadOnly(ctx, client, "v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { img.Close() })
	return img, reg, client
}

// chunkBuf builds a len(fills)*ovChunk buffer; fills[i]==0 leaves chunk i zero.
func chunkBuf(fills ...byte) []byte {
	buf := make([]byte, len(fills)*ovChunk)
	for i, f := range fills {
		if f != 0 {
			for j := 0; j < ovChunk; j++ {
				buf[i*ovChunk+j] = f
			}
		}
	}
	return buf
}

// TestOverlayRoundTripDelta is the headline: edit a frozen base through an
// overlay, Commit to a new tag, and prove only the CHANGED chunks were pushed
// while unchanged ones were reused by digest.
func TestOverlayRoundTripDelta(t *testing.T) {
	ctx := context.Background()
	// Base v1: chunk0=0xA1 data, chunk1=hole, chunk2=hole, chunk3=0xD4 data.
	base, reg, client := freezeBase(t, chunkBuf(0xA1, 0, 0, 0xD4))
	// v1 pushed: A1 + D4 + config = 3.
	if reg.pushCount != 3 {
		t.Fatalf("v1 pushCount=%d, want 3", reg.pushCount)
	}

	ov := oci.NewOverlay(base)

	// Modify chunk0 (0xA1 -> 0xB2) and fill chunk1 (was a hole -> 0xC3). Leave
	// chunk2 (hole) and chunk3 (0xD4) untouched.
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xB2}, ovChunk), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xC3}, ovChunk), ovChunk); err != nil {
		t.Fatal(err)
	}

	before := reg.pushCount
	if _, err := ov.Commit(ctx, client, "v2"); err != nil {
		t.Fatal(err)
	}
	// Delta = 2 changed chunks (B2, C3) + 1 new config = 3 pushes. chunk3 (D4)
	// reused by digest, chunk2 hole — neither pushed.
	if delta := reg.pushCount - before; delta != 3 {
		t.Fatalf("commit delta pushCount=%d, want 3 (2 changed + config)", delta)
	}

	// Reopen v2 and verify the merged content.
	img2, err := oci.OpenReadOnly(ctx, client, "v2")
	if err != nil {
		t.Fatal(err)
	}
	defer img2.Close()
	if sz, _ := img2.Size(); sz != 4*ovChunk {
		t.Fatalf("v2 size=%d", sz)
	}
	want := chunkBuf(0xB2, 0xC3, 0, 0xD4)
	got := make([]byte, len(want))
	if _, err := img2.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("v2 merged content mismatch")
	}
	// v1 is unchanged (immutability of the base tag).
	g1 := make([]byte, ovChunk)
	if _, err := base.ReadAt(g1, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g1, bytes.Repeat([]byte{0xA1}, ovChunk)) {
		t.Fatal("base v1 chunk0 changed — overlay mutated the base")
	}
}

// TestOverlayPoolReadWrite is the end-to-end demo: a frozen pool image, made
// writable by an overlay, edited through pool.OpenWith, committed to a new tag,
// and reopened with both the original and the new data intact.
func TestOverlayPoolReadWrite(t *testing.T) {
	ctx := context.Background()
	const blockSize = 4096

	// Build a pool, write W1, flush to the backing.
	mb := &memBacking{}
	p, err := pool.CreateWith(mb, 2<<20, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	v, err := p.CreateVolume("data", 512*1024)
	if err != nil {
		t.Fatal(err)
	}
	w1 := bytes.Repeat([]byte("ONE"), 700)
	if _, err := v.WriteAt(w1, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Freeze the pool backing as v1.
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	if _, err := oci.Freeze(ctx, &memReadOnly{buf: append([]byte(nil), mb.buf...)}, client, "v1", oci.Options{ChunkSize: ovChunk}); err != nil {
		t.Fatal(err)
	}

	// Open v1, overlay it, mount a R/W pool, write W2 at a new offset.
	base, err := oci.OpenReadOnly(ctx, client, "v1")
	if err != nil {
		t.Fatal(err)
	}
	ov := oci.NewOverlay(base)
	p2, err := pool.OpenWith(ov) // fully read-write over the frozen base
	if err != nil {
		t.Fatal(err)
	}
	v2, err := p2.OpenVolume("data")
	if err != nil {
		t.Fatal(err)
	}
	w2 := bytes.Repeat([]byte("TWO"), 500)
	if _, err := v2.WriteAt(w2, 256*1024); err != nil {
		t.Fatal(err)
	}
	if err := p2.Close(); err != nil { // flush updated metadata + data into the overlay
		t.Fatal(err)
	}

	// Commit the overlay to v2.
	if _, err := ov.Commit(ctx, client, "v2"); err != nil {
		t.Fatal(err)
	}

	// Reopen v2 read-only and verify BOTH writes survived.
	img2, err := oci.OpenReadOnly(ctx, client, "v2")
	if err != nil {
		t.Fatal(err)
	}
	defer img2.Close()
	p3, err := pool.OpenWith(img2)
	if err != nil {
		t.Fatal(err)
	}
	v3, err := p3.OpenVolume("data")
	if err != nil {
		t.Fatal(err)
	}
	g1 := make([]byte, len(w1))
	if _, err := v3.ReadAt(g1, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g1, w1) {
		t.Fatal("W1 (from the frozen base) lost after commit")
	}
	g2 := make([]byte, len(w2))
	if _, err := v3.ReadAt(g2, 256*1024); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g2, w2) {
		t.Fatal("W2 (written through the overlay) lost after commit")
	}
}

func TestOverlaySizeSyncClose(t *testing.T) {
	base, _, _ := freezeBase(t, chunkBuf(0x11))
	ov := oci.NewOverlay(base)
	if sz, _ := ov.Size(); sz != ovChunk {
		t.Fatalf("size=%d", sz)
	}
	if err := ov.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := ov.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOverlayReadWriteNegativeOffset(t *testing.T) {
	base, _, _ := freezeBase(t, chunkBuf(0x11))
	ov := oci.NewOverlay(base)
	if _, err := ov.ReadAt(make([]byte, 4), -1); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("ReadAt(-1): %v", err)
	}
	if _, err := ov.WriteAt([]byte{1}, -1); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("WriteAt(-1): %v", err)
	}
}

func TestOverlayReadPastEnd(t *testing.T) {
	base, _, _ := freezeBase(t, chunkBuf(0x11)) // 1 chunk
	ov := oci.NewOverlay(base)
	buf := make([]byte, ovChunk+50)
	n, err := ov.ReadAt(buf, 0)
	if n != ovChunk || !errors.Is(err, io.EOF) {
		t.Fatalf("read past end: n=%d err=%v", n, err)
	}
}

func TestOverlayWriteGrowAndReadBeyondBaseZero(t *testing.T) {
	base, _, _ := freezeBase(t, chunkBuf(0x11)) // 1 chunk, size = ovChunk
	ov := oci.NewOverlay(base)
	// Write a new chunk at index 2 (grows to 3 chunks); index 1 is now a region
	// past the base that was never written -> reads zero (readChunk beyond base).
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0x99}, ovChunk), 2*ovChunk); err != nil {
		t.Fatal(err)
	}
	if sz, _ := ov.Size(); sz != 3*ovChunk {
		t.Fatalf("grown size=%d", sz)
	}
	mid := make([]byte, ovChunk)
	if _, err := ov.ReadAt(mid, ovChunk); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mid, make([]byte, ovChunk)) {
		t.Fatal("region past base did not read zero")
	}
	got := make([]byte, ovChunk)
	if _, err := ov.ReadAt(got, 2*ovChunk); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{0x99}, ovChunk)) {
		t.Fatal("written grown chunk mismatch")
	}
}

func TestOverlayReadTailClamp(t *testing.T) {
	// Base size = one chunk + a 100-byte partial tail; a read starting mid-tail
	// with an over-long buffer must clamp to the logical size, then EOF.
	buf := make([]byte, ovChunk+100)
	for i := range buf {
		buf[i] = 0x33
	}
	base, _, _ := freezeBase(t, buf)
	ov := oci.NewOverlay(base)
	got := make([]byte, 300)
	n, err := ov.ReadAt(got, ovChunk+50)
	if n != 50 || !errors.Is(err, io.EOF) {
		t.Fatalf("tail clamp: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got[:50], bytes.Repeat([]byte{0x33}, 50)) {
		t.Fatal("tail content mismatch")
	}
}

func TestOverlayPartialWriteMaterialises(t *testing.T) {
	// A partial write to an existing data chunk must preserve the rest of it.
	base, _, _ := freezeBase(t, chunkBuf(0x11))
	ov := oci.NewOverlay(base)
	if _, err := ov.WriteAt([]byte{0xFF, 0xFF}, 10); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, ovChunk)
	if _, err := ov.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if got[10] != 0xFF || got[11] != 0xFF || got[0] != 0x11 || got[12] != 0x11 {
		t.Fatalf("partial write did not preserve the chunk: %x", got[:13])
	}
}

func TestOverlayTruncateShrinkAndGrow(t *testing.T) {
	base, _, _ := freezeBase(t, chunkBuf(0x11, 0x22, 0x33))
	ov := oci.NewOverlay(base)
	// Dirty chunks 1 and 2.
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xAA}, ovChunk), ovChunk); err != nil {
		t.Fatal(err)
	}
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xBB}, ovChunk), 2*ovChunk); err != nil {
		t.Fatal(err)
	}
	// Shrink to within chunk 0: chunks 1 and 2 (dirty) are dropped.
	if err := ov.Truncate(ovChunk); err != nil {
		t.Fatal(err)
	}
	if sz, _ := ov.Size(); sz != ovChunk {
		t.Fatalf("shrunk size=%d", sz)
	}
	if _, err := ov.ReadAt(make([]byte, 1), ovChunk); !errors.Is(err, io.EOF) {
		t.Fatalf("read past shrunk size: %v", err)
	}
	// Grow back: the dropped region reads zero now.
	if err := ov.Truncate(2 * ovChunk); err != nil {
		t.Fatal(err)
	}
	g := make([]byte, ovChunk)
	if _, err := ov.ReadAt(g, ovChunk); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g, make([]byte, ovChunk)) {
		t.Fatal("regrown region not zero")
	}
	if err := ov.Truncate(-1); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Truncate(-1): %v", err)
	}
}

func TestOverlayReadChunkPullError(t *testing.T) {
	base, reg, _ := freezeBase(t, chunkBuf(0x11))
	reg.mu.Lock()
	reg.blobs = map[string][]byte{} // drop blobs so the base chunk pull 404s
	reg.mu.Unlock()
	ov := oci.NewOverlay(base)
	if _, err := ov.ReadAt(make([]byte, 10), 0); err == nil {
		t.Fatal("expected base chunk pull error on read")
	}
}

func TestOverlayWriteChunkPullError(t *testing.T) {
	base, reg, _ := freezeBase(t, chunkBuf(0x11))
	reg.mu.Lock()
	reg.blobs = map[string][]byte{}
	reg.mu.Unlock()
	ov := oci.NewOverlay(base)
	// A PARTIAL write materialises the base chunk first -> pull fails.
	if _, err := ov.WriteAt([]byte{0xFF}, 10); err == nil {
		t.Fatal("expected base chunk pull error on partial write")
	}
}

func TestCommitAllZeroOverwriteBecomesHole(t *testing.T) {
	ctx := context.Background()
	base, reg, client := freezeBase(t, chunkBuf(0x11, 0x22)) // 2 data chunks
	ov := oci.NewOverlay(base)
	// Overwrite chunk0 with zeros -> it must become a hole (stored as nothing).
	if _, err := ov.WriteAt(make([]byte, ovChunk), 0); err != nil {
		t.Fatal(err)
	}
	before := reg.pushCount
	if _, err := ov.Commit(ctx, client, "v2"); err != nil {
		t.Fatal(err)
	}
	// Only the config is pushed: chunk0 became a hole (no push), chunk1 reused.
	if delta := reg.pushCount - before; delta != 1 {
		t.Fatalf("all-zero overwrite delta=%d, want 1 (config only)", delta)
	}
	img2, err := oci.OpenReadOnly(ctx, client, "v2")
	if err != nil {
		t.Fatal(err)
	}
	defer img2.Close()
	z := make([]byte, ovChunk)
	if _, err := img2.ReadAt(z, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(z, make([]byte, ovChunk)) {
		t.Fatal("zeroed chunk did not read as a hole")
	}
}

func TestCommitGrowShrink(t *testing.T) {
	ctx := context.Background()
	base, _, client := freezeBase(t, chunkBuf(0x11, 0x22))

	// Grow: write a chunk at index 3.
	ovG := oci.NewOverlay(base)
	if _, err := ovG.WriteAt(bytes.Repeat([]byte{0x44}, ovChunk), 3*ovChunk); err != nil {
		t.Fatal(err)
	}
	if _, err := ovG.Commit(ctx, client, "grown"); err != nil {
		t.Fatal(err)
	}
	ig, err := oci.OpenReadOnly(ctx, client, "grown")
	if err != nil {
		t.Fatal(err)
	}
	defer ig.Close()
	if sz, _ := ig.Size(); sz != 4*ovChunk {
		t.Fatalf("grown size=%d", sz)
	}

	// Shrink: truncate to one chunk.
	ovS := oci.NewOverlay(base)
	if err := ovS.Truncate(ovChunk); err != nil {
		t.Fatal(err)
	}
	if _, err := ovS.Commit(ctx, client, "shrunk"); err != nil {
		t.Fatal(err)
	}
	is, err := oci.OpenReadOnly(ctx, client, "shrunk")
	if err != nil {
		t.Fatal(err)
	}
	defer is.Close()
	if sz, _ := is.Size(); sz != ovChunk {
		t.Fatalf("shrunk size=%d", sz)
	}
}

func TestCommitReuseBaseTailAfterGrow(t *testing.T) {
	ctx := context.Background()
	// Base size = one full chunk + a 100-byte partial tail chunk.
	buf := make([]byte, ovChunk+100)
	for i := range buf {
		buf[i] = 0x55
	}
	base, _, client := freezeBase(t, buf)
	ov := oci.NewOverlay(base)
	// Grow well past the base WITHOUT touching the partial tail chunk (index 1):
	// Commit must reuse its digest with the BASE's 100-byte blob length.
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0x66}, ovChunk), 3*ovChunk); err != nil {
		t.Fatal(err)
	}
	if _, err := ov.Commit(ctx, client, "v2"); err != nil {
		t.Fatal(err)
	}
	img2, err := oci.OpenReadOnly(ctx, client, "v2")
	if err != nil {
		t.Fatal(err)
	}
	defer img2.Close()
	// The reused partial tail's 100 real bytes survive; the rest of that chunk
	// reads zero (sparse grow).
	g := make([]byte, ovChunk)
	if _, err := img2.ReadAt(g, ovChunk); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !bytes.Equal(g[:100], bytes.Repeat([]byte{0x55}, 100)) {
		t.Fatal("reused partial tail bytes lost")
	}
	if !bytes.Equal(g[100:], make([]byte, ovChunk-100)) {
		t.Fatal("grown remainder of the tail chunk not zero")
	}
}

func TestCommitDedupIdenticalDirtyChunks(t *testing.T) {
	ctx := context.Background()
	base, reg, client := freezeBase(t, chunkBuf(0x11, 0x22))
	ov := oci.NewOverlay(base)
	// Write IDENTICAL new content to both chunks -> one blob, one layer.
	same := bytes.Repeat([]byte{0x77}, ovChunk)
	if _, err := ov.WriteAt(same, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ov.WriteAt(same, ovChunk); err != nil {
		t.Fatal(err)
	}
	before := reg.pushCount
	if _, err := ov.Commit(ctx, client, "v2"); err != nil {
		t.Fatal(err)
	}
	// One unique chunk blob (the 2nd dedups at the registry HEAD) + config = 2.
	if delta := reg.pushCount - before; delta != 2 {
		t.Fatalf("identical-chunk commit delta=%d, want 2 (one blob + config)", delta)
	}
}

func TestCommitPushChunkFails(t *testing.T) {
	ctx := context.Background()
	base, _, _ := freezeBase(t, chunkBuf(0x11))
	ov := oci.NewOverlay(base)
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xEE}, ovChunk), 0); err != nil {
		t.Fatal(err)
	}
	dst := &registry.Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &failingDoer{failChunkPush: true}}
	if _, err := ov.Commit(ctx, dst, "v2"); err == nil {
		t.Fatal("expected chunk push failure")
	}
}

func TestCommitPushConfigFails(t *testing.T) {
	ctx := context.Background()
	base, _, _ := freezeBase(t, chunkBuf(0x11))
	ov := oci.NewOverlay(base)
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xEE}, ovChunk), 0); err != nil {
		t.Fatal(err)
	}
	dst := &registry.Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &failingDoer{failConfig: true}}
	if _, err := ov.Commit(ctx, dst, "v2"); err == nil {
		t.Fatal("expected config push failure")
	}
}

func TestCommitManifestFails(t *testing.T) {
	ctx := context.Background()
	base, _, _ := freezeBase(t, chunkBuf(0x11))
	ov := oci.NewOverlay(base)
	if _, err := ov.WriteAt(bytes.Repeat([]byte{0xEE}, ovChunk), 0); err != nil {
		t.Fatal(err)
	}
	dst := &registry.Client{BaseURL: "http://x", Repository: "repo", HTTPClient: manifestFailDoer{}}
	if _, err := ov.Commit(ctx, dst, "v2"); err == nil {
		t.Fatal("expected manifest put failure")
	}
}
