// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package oci_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-volumes/oci"
	"github.com/go-volumes/oci/registry"
)

// errSizeRO is a ReadOnly whose Size fails.
type errSizeRO struct{}

func (errSizeRO) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (errSizeRO) Size() (int64, error)              { return 0, errors.New("size boom") }
func (errSizeRO) Close() error                      { return nil }

// negSizeRO reports a negative size.
type negSizeRO struct{}

func (negSizeRO) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (negSizeRO) Size() (int64, error)              { return -1, nil }
func (negSizeRO) Close() error                      { return nil }

// readErrRO fails on ReadAt for a non-empty source.
type readErrRO struct{ size int64 }

func (r readErrRO) ReadAt([]byte, int64) (int, error) { return 0, errors.New("read boom") }
func (r readErrRO) Size() (int64, error)              { return r.size, nil }
func (readErrRO) Close() error                        { return nil }

// roBytes is a minimal byte-backed ReadOnly.
type roBytes struct{ b []byte }

func (r roBytes) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (r roBytes) Size() (int64, error) { return int64(len(r.b)), nil }
func (roBytes) Close() error           { return nil }

// eofWithDataRO returns (n, io.EOF) on the final read even when p is fully
// filled, the legal io.ReaderAt behaviour Freeze's readFull must tolerate.
type eofWithDataRO struct{ b []byte }

func (r eofWithDataRO) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	if off+int64(n) >= int64(len(r.b)) {
		return n, io.EOF // EOF together with the last bytes
	}
	return n, nil
}
func (r eofWithDataRO) Size() (int64, error) { return int64(len(r.b)), nil }
func (eofWithDataRO) Close() error           { return nil }

func TestFreezeEOFWithData(t *testing.T) {
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	// 4096-byte source (exactly one full chunk) whose read returns EOF+data.
	if _, err := oci.Freeze(context.Background(), eofWithDataRO{b: bytes.Repeat([]byte{7}, 4096)},
		client, "v1", oci.Options{ChunkSize: 4096}); err != nil {
		t.Fatal(err)
	}
}

func TestFreezeBadChunkSize(t *testing.T) {
	_, err := oci.Freeze(context.Background(), roBytes{b: []byte("x")}, nil, "r",
		oci.Options{ChunkSize: 3}) // not a power of two
	if err == nil || !strings.Contains(err.Error(), "power of two") {
		t.Fatalf("expected power-of-two error, got %v", err)
	}
}

func TestFreezeSizeError(t *testing.T) {
	if _, err := oci.Freeze(context.Background(), errSizeRO{}, nil, "r", oci.Options{ChunkSize: 4096}); err == nil {
		t.Fatal("expected source size error")
	}
}

func TestFreezeNegativeSize(t *testing.T) {
	if _, err := oci.Freeze(context.Background(), negSizeRO{}, nil, "r", oci.Options{ChunkSize: 4096}); err == nil {
		t.Fatal("expected negative size error")
	}
}

func TestFreezeReadError(t *testing.T) {
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	if _, err := oci.Freeze(context.Background(), readErrRO{size: 4096}, client, "r", oci.Options{ChunkSize: 4096}); err == nil {
		t.Fatal("expected chunk read error")
	}
}

// failingDoer fails pushes (or specific methods) to exercise Freeze's push paths.
type failingDoer struct {
	failChunkPush bool // fail the PUT of a non-config blob
	failConfig    bool // fail the PUT of the config blob (last push)
	puts          int
}

func (d *failingDoer) Do(r *http.Request) (*http.Response, error) {
	resp := func(code int, hdr http.Header, body string) *http.Response {
		if hdr == nil {
			hdr = http.Header{}
		}
		return &http.Response{StatusCode: code, Header: hdr, Body: io.NopCloser(strings.NewReader(body))}
	}
	switch r.Method {
	case http.MethodHead:
		return resp(404, nil, ""), nil
	case http.MethodPost:
		return resp(202, http.Header{"Location": {"/v2/repo/uploads-put/1"}}, ""), nil
	case http.MethodPut:
		if strings.Contains(r.URL.Path, "/manifests/") {
			return resp(201, http.Header{"Docker-Content-Digest": {"sha256:" + strings.Repeat("0", 64)}}, ""), nil
		}
		d.puts++
		if d.failChunkPush {
			return resp(500, nil, `{"errors":[{"code":"X"}]}`), nil
		}
		if d.failConfig {
			// First PUT (chunk) succeeds, second (config) fails. With one unique
			// non-zero chunk, the config is the 2nd blob PUT.
			if d.puts >= 2 {
				return resp(500, nil, `{"errors":[{"code":"CFG"}]}`), nil
			}
		}
		return resp(201, nil, ""), nil
	}
	return resp(500, nil, ""), nil
}

func TestFreezePushChunkFails(t *testing.T) {
	client := &registry.Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &failingDoer{failChunkPush: true}}
	if _, err := oci.Freeze(context.Background(), roBytes{b: []byte("data")}, client, "r", oci.Options{ChunkSize: 4096}); err == nil {
		t.Fatal("expected chunk push failure")
	}
}

func TestFreezePushConfigFails(t *testing.T) {
	client := &registry.Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &failingDoer{failConfig: true}}
	if _, err := oci.Freeze(context.Background(), roBytes{b: []byte("data")}, client, "r", oci.Options{ChunkSize: 4096}); err == nil {
		t.Fatal("expected config push failure")
	}
}

// manifestFailDoer pushes blobs fine but fails the manifest PUT.
type manifestFailDoer struct{}

func (manifestFailDoer) Do(r *http.Request) (*http.Response, error) {
	mk := func(code int, hdr http.Header, body string) *http.Response {
		if hdr == nil {
			hdr = http.Header{}
		}
		return &http.Response{StatusCode: code, Header: hdr, Body: io.NopCloser(strings.NewReader(body))}
	}
	switch r.Method {
	case http.MethodHead:
		return mk(404, nil, ""), nil
	case http.MethodPost:
		return mk(202, http.Header{"Location": {"/v2/repo/uploads-put/1"}}, ""), nil
	case http.MethodPut:
		if strings.Contains(r.URL.Path, "/manifests/") {
			return mk(400, nil, `{"errors":[{"code":"MANIFEST_INVALID"}]}`), nil
		}
		return mk(201, nil, ""), nil
	}
	return mk(500, nil, ""), nil
}

func TestFreezeManifestPutFails(t *testing.T) {
	client := &registry.Client{BaseURL: "http://x", Repository: "repo", HTTPClient: manifestFailDoer{}}
	if _, err := oci.Freeze(context.Background(), roBytes{b: []byte("data")}, client, "r", oci.Options{ChunkSize: 4096}); err == nil {
		t.Fatal("expected manifest put failure")
	}
}

// ---- OpenReadOnly error branches ---------------------------------------

func TestOpenManifestNotFound(t *testing.T) {
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "absent"); err == nil {
		t.Fatal("expected manifest get failure")
	}
}

// rawSrv serves arbitrary manifest/config bodies to drive OpenReadOnly parsing.
type rawSrv struct {
	manifest string
	config   []byte // bytes returned for the config blob GET (digest-addressed)
}

func startRawSrv(t *testing.T, rs *rawSrv) (*registry.Client, func()) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", registry.MediaTypeManifest)
			w.Write([]byte(rs.manifest))
		case strings.Contains(r.URL.Path, "/blobs/"):
			digest := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			// Serve the config bytes only when the digest matches them, so
			// PullBlob's verification passes for the happy fields.
			if rs.config != nil && digest == registry.Digest(rs.config) {
				w.Write(rs.config)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"errors":[{"code":"BLOB_UNKNOWN"}]}`))
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	return &registry.Client{BaseURL: srv.URL, Repository: "repo"}, srv.Close
}

func mkManifest(t *testing.T, configDigest string, configSize int64) string {
	t.Helper()
	m := map[string]any{
		"schemaVersion": 2,
		"mediaType":     registry.MediaTypeManifest,
		"config":        map[string]any{"mediaType": oci.ConfigMediaType, "digest": configDigest, "size": configSize},
		"layers":        []any{},
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestOpenBadManifestJSON(t *testing.T) {
	client, closeFn := startRawSrv(t, &rawSrv{manifest: "{not json"})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil {
		t.Fatal("expected manifest parse error")
	}
}

func TestOpenManifestNoConfig(t *testing.T) {
	client, closeFn := startRawSrv(t, &rawSrv{manifest: `{"schemaVersion":2,"config":{"digest":""}}`})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil ||
		!strings.Contains(err.Error(), "no config") {
		t.Fatalf("expected no-config error, got %v", err)
	}
}

func TestOpenConfigPullFails(t *testing.T) {
	// Manifest references a config digest the registry does not have.
	missing := registry.Digest([]byte("missing-config"))
	client, closeFn := startRawSrv(t, &rawSrv{manifest: mkManifest(t, missing, 10)})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil {
		t.Fatal("expected config pull failure")
	}
}

func TestOpenBadConfigJSON(t *testing.T) {
	cfg := []byte("{not json")
	client, closeFn := startRawSrv(t, &rawSrv{manifest: mkManifest(t, registry.Digest(cfg), int64(len(cfg))), config: cfg})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil {
		t.Fatal("expected config parse error")
	}
}

func TestOpenNegativeConfigSize(t *testing.T) {
	cfg, _ := json.Marshal(oci.Config{Size: -1, ChunkSize: 4096})
	client, closeFn := startRawSrv(t, &rawSrv{manifest: mkManifest(t, registry.Digest(cfg), int64(len(cfg))), config: cfg})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil ||
		!strings.Contains(err.Error(), "negative size") {
		t.Fatalf("expected negative size error, got %v", err)
	}
}

func TestOpenBadConfigChunkSize(t *testing.T) {
	cfg, _ := json.Marshal(oci.Config{Size: 10, ChunkSize: 3}) // not power of two
	client, closeFn := startRawSrv(t, &rawSrv{manifest: mkManifest(t, registry.Digest(cfg), int64(len(cfg))), config: cfg})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil ||
		!strings.Contains(err.Error(), "power of two") {
		t.Fatalf("expected chunk-size error, got %v", err)
	}
}

func TestOpenChunkCountMismatch(t *testing.T) {
	// Size implies 1 chunk but the index lists 2.
	cfg, _ := json.Marshal(oci.Config{Size: 10, ChunkSize: 4096, Chunks: []string{"", ""}})
	client, closeFn := startRawSrv(t, &rawSrv{manifest: mkManifest(t, registry.Digest(cfg), int64(len(cfg))), config: cfg})
	defer closeFn()
	if _, err := oci.OpenReadOnly(context.Background(), client, "v1"); err == nil ||
		!strings.Contains(err.Error(), "chunk digests") {
		t.Fatalf("expected chunk-count mismatch, got %v", err)
	}
}

// ---- ReadAt error branches ---------------------------------------------

func TestImageReadNegativeOffset(t *testing.T) {
	img := freezeOne(t, []byte("hello world tiny"), 4096)
	if _, err := img.ReadAt(make([]byte, 4), -1); err == nil ||
		!strings.Contains(err.Error(), "negative") {
		t.Fatalf("expected negative offset error, got %v", err)
	}
}

func TestImageReadPastEndClamps(t *testing.T) {
	// A read buffer longer than the image clamps to size within the final chunk
	// and then returns io.EOF.
	src := []byte("hello world tiny") // 16 bytes, one chunk
	img := freezeOne(t, src, 4096)
	buf := make([]byte, 20) // longer than the 16-byte image
	n, err := img.ReadAt(buf, 0)
	if n != len(src) || !errors.Is(err, io.EOF) {
		t.Fatalf("read past end: n=%d err=%v", n, err)
	}
	if !strings.HasPrefix(string(buf[:n]), "hello world") {
		t.Fatalf("clamped read content: %q", buf[:n])
	}
}

func TestImageReadChunkPullFails(t *testing.T) {
	// Freeze a real chunk, then point the image at a registry that lost the blob.
	ctx := context.Background()
	reg := newReg()
	client, closeFn := startReg(t, reg)
	defer closeFn()
	if _, err := oci.Freeze(ctx, roBytes{b: []byte(strings.Repeat("Z", 100))}, client, "v1", oci.Options{ChunkSize: 4096}); err != nil {
		t.Fatal(err)
	}
	img, err := oci.OpenReadOnly(ctx, client, "v1")
	if err != nil {
		t.Fatal(err)
	}
	// Drop all blobs so the chunk pull 404s.
	reg.mu.Lock()
	for k := range reg.blobs {
		// keep the config/manifest reachability via manifests map; clear chunks.
		_ = k
	}
	reg.blobs = map[string][]byte{}
	reg.mu.Unlock()
	if _, err := img.ReadAt(make([]byte, 10), 0); err == nil {
		t.Fatal("expected chunk pull failure")
	}
}

func TestImageReadCachedSecondTime(t *testing.T) {
	// Two reads of the same chunk exercise the LRU cache-hit path.
	img := freezeOne(t, []byte(strings.Repeat("C", 200)), 4096)
	b1 := make([]byte, 50)
	if _, err := img.ReadAt(b1, 0); err != nil {
		t.Fatal(err)
	}
	b2 := make([]byte, 50)
	if _, err := img.ReadAt(b2, 10); err != nil { // same chunk, now cached
		t.Fatal(err)
	}
}

// freezeOne freezes data to a fresh in-memory registry and returns the image.
func freezeOne(t *testing.T, data []byte, chunk int64) *oci.Image {
	t.Helper()
	ctx := context.Background()
	reg := newReg()
	client, closeFn := startReg(t, reg)
	t.Cleanup(closeFn)
	if _, err := oci.Freeze(ctx, roBytes{b: data}, client, "v1", oci.Options{ChunkSize: chunk}); err != nil {
		t.Fatal(err)
	}
	img, err := oci.OpenReadOnly(ctx, client, "v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { img.Close() })
	return img
}

func TestLRUEviction(t *testing.T) {
	// Force eviction: freeze many distinct chunks, read them all, then re-read
	// the first to drive a miss-after-evict (the cache cap is small).
	const nchunks = 40
	data := make([]byte, 4096*nchunks)
	for c := 0; c < nchunks; c++ {
		// Stamp each chunk with its index so every chunk is a DISTINCT blob,
		// forcing the LRU past its capacity (eviction) rather than deduping.
		for i := 0; i < 4096; i++ {
			data[c*4096+i] = byte(c)
		}
	}
	img := freezeOne(t, data, 4096)
	// Read all chunks (more than the cache capacity) then the first again.
	buf := make([]byte, 16)
	for off := int64(0); off < int64(len(data)); off += 4096 {
		if _, err := img.ReadAt(buf, off); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := img.ReadAt(buf, 0); err != nil { // re-pull after eviction
		t.Fatal(err)
	}
}
