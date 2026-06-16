// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeRegistry is an in-memory OCI Distribution v2 server for tests. It supports
// monolithic blob upload, blob HEAD/GET, manifest PUT/GET, and an optional
// bearer-token handshake.
type fakeRegistry struct {
	mu        sync.Mutex
	blobs     map[string][]byte // digest -> bytes
	manifests map[string][]byte // ref    -> body
	mtypes    map[string]string // ref    -> media type
	uploads   map[string][]byte // upload id -> accumulated bytes

	requireToken bool   // demand a bearer token (401 handshake)
	tokenValue   string // the token the token endpoint hands out
	wantUser     string // basic creds the token endpoint requires (optional)
	wantPass     string

	pushCount int // number of successful blob PUTs (dedup assertion)
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		blobs:      map[string][]byte{},
		manifests:  map[string][]byte{},
		mtypes:     map[string]string{},
		uploads:    map[string][]byte{},
		tokenValue: "test-token",
	}
}

func (f *fakeRegistry) handler() http.Handler {
	mux := http.NewServeMux()

	// Token endpoint.
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if f.wantUser != "" {
			u, p, ok := r.BasicAuth()
			if !ok || u != f.wantUser || p != f.wantPass {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		fmt.Fprintf(w, `{"token":%q}`, f.tokenValue)
	})

	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		// Bearer gate.
		if f.requireToken {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+f.tokenValue {
				w.Header().Set("WWW-Authenticate",
					fmt.Sprintf(`Bearer realm="http://%s/token",service="reg",scope="repository:repo:pull,push"`, r.Host))
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		f.route(w, r)
	})
	return mux
}

func (f *fakeRegistry) route(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case strings.Contains(p, "/blobs/uploads/"):
		f.handleUploadStart(w, r)
	case strings.Contains(p, "/blobs/uploads"): // unreachable guard
		http.NotFound(w, r)
	case strings.HasPrefix(p, "/v2/") && strings.Contains(p, "/uploads-put/"):
		f.handleUploadPut(w, r)
	case strings.Contains(p, "/blobs/"):
		f.handleBlob(w, r)
	case strings.Contains(p, "/manifests/"):
		f.handleManifest(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeRegistry) handleUploadStart(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		f.mu.Lock()
		id := fmt.Sprintf("u%d", len(f.uploads)+1)
		f.uploads[id] = nil
		f.mu.Unlock()
		// Point the client at a PUT location carrying the upload id.
		w.Header().Set("Location", "/v2/repo/uploads-put/"+id)
		w.WriteHeader(http.StatusAccepted)
	default:
		http.Error(w, `{"errors":[{"code":"UNSUPPORTED"}]}`, http.StatusMethodNotAllowed)
	}
}

func (f *fakeRegistry) handleUploadPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.NotFound(w, r)
		return
	}
	id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	digest := r.URL.Query().Get("digest")
	body, _ := io.ReadAll(r.Body)
	if Digest(body) != digest {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"errors":[{"code":"DIGEST_INVALID","message":"digest mismatch"}]}`)
		return
	}
	f.mu.Lock()
	f.blobs[digest] = body
	delete(f.uploads, id)
	f.pushCount++
	f.mu.Unlock()
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeRegistry) handleBlob(w http.ResponseWriter, r *http.Request) {
	digest := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	f.mu.Lock()
	b, ok := f.blobs[digest]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"errors":[{"code":"BLOB_UNKNOWN","message":"not here"}]}`)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Write(b)
}

func (f *fakeRegistry) handleManifest(w http.ResponseWriter, r *http.Request) {
	ref := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.manifests[ref] = body
		f.mtypes[ref] = r.Header.Get("Content-Type")
		d := Digest(body)
		f.manifests[d] = body // also addressable by digest
		f.mtypes[d] = r.Header.Get("Content-Type")
		f.mu.Unlock()
		w.Header().Set("Docker-Content-Digest", d)
		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		f.mu.Lock()
		body, ok := f.manifests[ref]
		mt := f.mtypes[ref]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"errors":[{"code":"MANIFEST_UNKNOWN","message":"no manifest"}]}`)
			return
		}
		w.Header().Set("Content-Type", mt)
		w.Write(body)
	default:
		http.NotFound(w, r)
	}
}

// newTestClient spins up a fakeRegistry-backed httptest server and a Client.
func newTestClient(t *testing.T, f *fakeRegistry) (*Client, func()) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	c := &Client{BaseURL: srv.URL, Repository: "repo"}
	return c, srv.Close
}

func TestPushPullBlobAndDedup(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	data := []byte("hello frozen world")
	dg, err := c.PushBlob(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if dg != Digest(data) {
		t.Fatalf("digest %s != %s", dg, Digest(data))
	}
	// Second push of identical content must be deduped (no new PUT).
	if _, err := c.PushBlob(ctx, data); err != nil {
		t.Fatal(err)
	}
	if f.pushCount != 1 {
		t.Fatalf("expected 1 PUT after dedup, got %d", f.pushCount)
	}

	ok, err := c.BlobExists(ctx, dg)
	if err != nil || !ok {
		t.Fatalf("BlobExists=%v err=%v", ok, err)
	}
	got, err := c.PullBlob(ctx, dg)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("pull mismatch: %q", got)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	body := []byte(`{"schemaVersion":2}`)
	dg, err := c.PutManifest(ctx, "v1", MediaTypeManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if dg != Digest(body) {
		t.Fatalf("manifest digest %s != %s", dg, Digest(body))
	}
	mt, got, err := c.GetManifest(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if mt != MediaTypeManifest || string(got) != string(body) {
		t.Fatalf("manifest mismatch: mt=%q body=%q", mt, got)
	}
}

func TestBearerTokenHandshake(t *testing.T) {
	f := newFakeRegistry()
	f.requireToken = true
	f.wantUser = "alice"
	f.wantPass = "secret"
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	c.Username = "alice"
	c.Password = "secret"
	ctx := context.Background()

	// First call triggers 401 -> token fetch -> retry. Subsequent calls reuse it.
	dg, err := c.PushBlob(ctx, []byte("auth me"))
	if err != nil {
		t.Fatalf("push under bearer: %v", err)
	}
	if c.token != f.tokenValue {
		t.Fatalf("token not cached: %q", c.token)
	}
	if _, err := c.PullBlob(ctx, dg); err != nil {
		t.Fatalf("pull under bearer: %v", err)
	}
}

func TestNotFoundSentinel(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	absent := Digest([]byte("nope"))
	_, err := c.PullBlob(ctx, absent)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	_, _, err = c.GetManifest(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for manifest, got %v", err)
	}
}

func TestDigestMismatchOnPull(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	// Plant a blob under a wrong digest key directly.
	real := []byte("payload")
	wrong := Digest([]byte("different"))
	f.blobs[wrong] = real
	_, err := c.PullBlob(ctx, wrong)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch, got %v", err)
	}
}

func TestBadDigestRejected(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo"}
	ctx := context.Background()
	for _, bad := range []string{"deadbeef", "sha256:zz", "sha256:" + strings.Repeat("0", 63)} {
		if _, err := c.BlobExists(ctx, bad); !errors.Is(err, ErrBadDigest) {
			t.Errorf("BlobExists(%q): expected ErrBadDigest, got %v", bad, err)
		}
		if _, err := c.PullBlob(ctx, bad); !errors.Is(err, ErrBadDigest) {
			t.Errorf("PullBlob(%q): expected ErrBadDigest, got %v", bad, err)
		}
	}
}

func TestAPIErrorEnvelopeAndRaw(t *testing.T) {
	// JSON envelope.
	e := newAPIError("GET", "http://x/v2/repo/blobs/y", 500,
		[]byte(`{"errors":[{"code":"INTERNAL","message":"boom"}]}`))
	if !strings.Contains(e.Error(), "INTERNAL") || !strings.Contains(e.Error(), "boom") {
		t.Errorf("envelope error string: %s", e.Error())
	}
	// Raw (non-JSON) body.
	e2 := newAPIError("PUT", "http://x", 502, []byte("bad gateway"))
	if !strings.Contains(e2.Error(), "bad gateway") {
		t.Errorf("raw error string: %s", e2.Error())
	}
	// Bare status.
	e3 := newAPIError("HEAD", "http://x", 418, nil)
	if !strings.Contains(e3.Error(), "418") {
		t.Errorf("bare error string: %s", e3.Error())
	}
	// errors.Is bridge.
	e4 := newAPIError("GET", "http://x", 404, nil)
	if !errors.Is(e4, ErrNotFound) {
		t.Errorf("404 APIError should be ErrNotFound")
	}
}
