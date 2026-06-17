// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
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

	// tags/list behaviour knobs.
	tagsNotFound bool     // tags/list returns 404 (unknown repo)
	tagsErr      int      // if non-zero, tags/list returns this status with an error body
	pageSize     int      // if >0, paginate tags/list this many at a time via Link
	tagOrder     []string // explicit tag ordering for tags/list (defaults to sorted)

	// manifest DELETE behaviour knobs.
	rejectTagDelete int      // if non-zero, DELETE by tag returns this status (405/400)
	deletedRefs     []string // refs the client successfully DELETEd, in order
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
	case strings.HasSuffix(p, "/tags/list"):
		f.handleTagsList(w, r)
	case strings.Contains(p, "/blobs/"):
		f.handleBlob(w, r)
	case strings.Contains(p, "/manifests/"):
		f.handleManifest(w, r)
	default:
		http.NotFound(w, r)
	}
}

// handleTagsList serves GET /v2/<repo>/tags/list, honouring the 404, error, and
// pagination knobs. Pagination emits a Link: <…?last=…&n=…>; rel="next" header.
func (f *fakeRegistry) handleTagsList(w http.ResponseWriter, r *http.Request) {
	if f.tagsNotFound {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"errors":[{"code":"NAME_UNKNOWN","message":"repo unknown"}]}`)
		return
	}
	if f.tagsErr != 0 {
		w.WriteHeader(f.tagsErr)
		fmt.Fprint(w, `{"errors":[{"code":"DENIED","message":"nope"}]}`)
		return
	}
	tags := f.tagOrder
	if tags == nil {
		f.mu.Lock()
		for ref := range f.manifests {
			if !strings.HasPrefix(ref, "sha256:") {
				tags = append(tags, ref)
			}
		}
		f.mu.Unlock()
		sort.Strings(tags)
	}
	// Pagination: slice [last, last+pageSize).
	start := 0
	if last := r.URL.Query().Get("last"); last != "" {
		for i, t := range tags {
			if t == last {
				start = i + 1
				break
			}
		}
	}
	end := len(tags)
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
	}
	page := tags[start:end]
	if f.pageSize > 0 && end < len(tags) {
		nextLast := tags[end-1]
		w.Header().Set("Link",
			fmt.Sprintf(`</v2/%s/tags/list?last=%s&n=%d>; rel="next"`, "repo", nextLast, f.pageSize))
	}
	w.Header().Set("Content-Type", "application/json")
	body, _ := json.Marshal(tagsListResponse{Name: "repo", Tags: page})
	w.Write(body)
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
	case http.MethodHead:
		f.mu.Lock()
		body, ok := f.manifests[ref]
		mt := f.mtypes[ref]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", mt)
		w.Header().Set("Docker-Content-Digest", Digest(body))
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		isTag := !strings.HasPrefix(ref, "sha256:")
		if isTag && f.rejectTagDelete != 0 {
			w.WriteHeader(f.rejectTagDelete)
			fmt.Fprint(w, `{"errors":[{"code":"UNSUPPORTED","message":"delete by tag unsupported"}]}`)
			return
		}
		f.mu.Lock()
		_, ok := f.manifests[ref]
		if ok {
			delete(f.manifests, ref)
			delete(f.mtypes, ref)
			f.deletedRefs = append(f.deletedRefs, ref)
		}
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"errors":[{"code":"MANIFEST_UNKNOWN","message":"no manifest"}]}`)
			return
		}
		w.WriteHeader(http.StatusAccepted)
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

func TestTagsListBasic(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	for _, tag := range []string{"2026-01-03", "2026-01-01", "2026-01-02"} {
		if _, err := c.PutManifest(ctx, tag, MediaTypeManifest, []byte(`{"schemaVersion":2,"t":"`+tag+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	tags, err := c.TagsList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"2026-01-01", "2026-01-02", "2026-01-03"}
	if strings.Join(tags, ",") != strings.Join(want, ",") {
		t.Fatalf("tags=%v want %v", tags, want)
	}
}

func TestTagsListPagination(t *testing.T) {
	f := newFakeRegistry()
	f.pageSize = 2
	f.tagOrder = []string{"a", "b", "c", "d", "e"}
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	tags, err := c.TagsList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(tags, ",") != "a,b,c,d,e" {
		t.Fatalf("paginated tags=%v", tags)
	}
}

func TestTagsListNotFoundIsEmpty(t *testing.T) {
	f := newFakeRegistry()
	f.tagsNotFound = true
	c, closeFn := newTestClient(t, f)
	defer closeFn()

	tags, err := c.TagsList(context.Background())
	if err != nil {
		t.Fatalf("404 tags/list should be empty, not error: %v", err)
	}
	if len(tags) != 0 {
		t.Fatalf("expected empty tags, got %v", tags)
	}
}

func TestTagsListServerError(t *testing.T) {
	f := newFakeRegistry()
	f.tagsErr = http.StatusInternalServerError
	c, closeFn := newTestClient(t, f)
	defer closeFn()

	_, err := c.TagsList(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 500 {
		t.Fatalf("expected 500 APIError, got %v", err)
	}
}

func TestTagsListTransportError(t *testing.T) {
	// A client pointed at a closed server surfaces the transport error.
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	closeFn() // close immediately
	if _, err := c.TagsList(context.Background()); err == nil {
		t.Fatal("expected transport error from closed server")
	}
}

func TestTagsListBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, Repository: "repo"}
	if _, err := c.TagsList(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "decode tags list") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestNextPageURL(t *testing.T) {
	cases := []struct {
		name, base, link, want string
		wantErr                bool
	}{
		{name: "empty", base: "http://x", link: "", want: ""},
		{name: "no-angle", base: "http://x", link: `rel="next"`, want: ""},
		{name: "not-next-rel", base: "http://x", link: `</next>; rel="prev"`, want: ""},
		{name: "next-quoted", base: "http://x", link: `</v2/repo/tags/list?last=c>; rel="next"`, want: "http://x/v2/repo/tags/list?last=c"},
		{name: "next-unquoted", base: "http://x", link: `</p?last=c>; rel=next`, want: "http://x/p?last=c"},
		{name: "bad-base", base: "http://\x7f", link: `</p>; rel="next"`, wantErr: true},
		{name: "bad-target", base: "http://x", link: "<http://\x7f>; rel=\"next\"", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nextPageURL(tc.base, tc.link)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.link)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestDeleteManifestByTag(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	if _, err := c.PutManifest(ctx, "v1", MediaTypeManifest, []byte(`{"schemaVersion":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteManifest(ctx, "v1"); err != nil {
		t.Fatalf("delete by tag: %v", err)
	}
	if len(f.deletedRefs) != 1 || f.deletedRefs[0] != "v1" {
		t.Fatalf("deletedRefs=%v", f.deletedRefs)
	}
	// A second delete hits a now-absent manifest -> ErrNotFound.
	if err := c.DeleteManifest(ctx, "v1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on re-delete, got %v", err)
	}
}

func TestDeleteManifestByDigest(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	body := []byte(`{"schemaVersion":2,"x":1}`)
	dg, err := c.PutManifest(ctx, "v1", MediaTypeManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	// Deleting by digest must not HEAD-resolve (it is already a digest).
	if err := c.DeleteManifest(ctx, dg); err != nil {
		t.Fatalf("delete by digest: %v", err)
	}
	if len(f.deletedRefs) != 1 || f.deletedRefs[0] != dg {
		t.Fatalf("deletedRefs=%v", f.deletedRefs)
	}
}

func TestDeleteManifestTagFallbackToDigest(t *testing.T) {
	// Registry refuses by-tag DELETE (405); client must HEAD then delete by digest.
	f := newFakeRegistry()
	f.rejectTagDelete = http.StatusMethodNotAllowed
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	body := []byte(`{"schemaVersion":2,"y":2}`)
	dg, err := c.PutManifest(ctx, "v1", MediaTypeManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteManifest(ctx, "v1"); err != nil {
		t.Fatalf("tag-delete fallback: %v", err)
	}
	// The successful DELETE recorded should be by digest, not by tag.
	if len(f.deletedRefs) != 1 || f.deletedRefs[0] != dg {
		t.Fatalf("expected digest delete, deletedRefs=%v (want %s)", f.deletedRefs, dg)
	}
}

func TestDeleteManifestTagFallback400(t *testing.T) {
	f := newFakeRegistry()
	f.rejectTagDelete = http.StatusBadRequest
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()

	dg, err := c.PutManifest(ctx, "v1", MediaTypeManifest, []byte(`{"schemaVersion":2,"z":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteManifest(ctx, "v1"); err != nil {
		t.Fatalf("400 fallback: %v", err)
	}
	if f.deletedRefs[0] != dg {
		t.Fatalf("expected digest delete, got %v", f.deletedRefs)
	}
}

func TestDeleteManifestNonRetryableError(t *testing.T) {
	// A 500 on a by-tag delete is NOT the tag-unsupported shape: surfaced as-is,
	// no HEAD fallback.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			t.Errorf("unexpected HEAD fallback for a 500 delete")
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"errors":[{"code":"INTERNAL"}]}`)
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, Repository: "repo"}
	err := c.DeleteManifest(context.Background(), "v1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 500 {
		t.Fatalf("expected 500 APIError, got %v", err)
	}
}

func TestDeleteManifestTransportError(t *testing.T) {
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	closeFn()
	if err := c.DeleteManifest(context.Background(), "v1"); err == nil {
		t.Fatal("expected transport error")
	}
}

func TestDeleteManifestHeadResolveFails(t *testing.T) {
	// By-tag DELETE returns 405, but the HEAD resolution then fails: the ORIGINAL
	// 405 error is surfaced (the HEAD failure is secondary).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprint(w, `{"errors":[{"code":"UNSUPPORTED"}]}`)
		case http.MethodHead:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, Repository: "repo"}
	err := c.DeleteManifest(context.Background(), "v1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected original 405 APIError, got %v", err)
	}
}

func TestDeleteManifestHeadNoDigest(t *testing.T) {
	// HEAD returns 200 but no Docker-Content-Digest header: resolveDigest errors,
	// so the original 405 is surfaced.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case http.MethodHead:
			w.WriteHeader(http.StatusOK) // no Docker-Content-Digest
		}
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, Repository: "repo"}
	err := c.DeleteManifest(context.Background(), "v1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected original 405 APIError, got %v", err)
	}
}

func TestResolveDigestDirect(t *testing.T) {
	// Cover resolveDigest's success path and the no-digest-header branch directly.
	f := newFakeRegistry()
	c, closeFn := newTestClient(t, f)
	defer closeFn()
	ctx := context.Background()
	body := []byte(`{"schemaVersion":2,"q":9}`)
	dg, _ := c.PutManifest(ctx, "v1", MediaTypeManifest, body)
	got, err := c.resolveDigest(ctx, "v1")
	if err != nil || got != dg {
		t.Fatalf("resolveDigest=%q err=%v want %s", got, err, dg)
	}
	// Absent ref -> 404 error.
	if _, err := c.resolveDigest(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestIsDigest(t *testing.T) {
	if !isDigest(Digest([]byte("x"))) {
		t.Fatal("a real digest should be recognised")
	}
	if isDigest("v1") {
		t.Fatal("a tag is not a digest")
	}
}

// errReadCloser yields a body whose Read always fails, to exercise the
// io.ReadAll error branch on a 200 response.
type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("boom-read") }
func (errReadCloser) Close() error             { return nil }

// scriptedDoer returns canned responses per attempt index, then transport errors.
type scriptedDoer struct {
	resps []*http.Response
	errs  []error
	i     int
}

func (d *scriptedDoer) Do(*http.Request) (*http.Response, error) {
	i := d.i
	d.i++
	if i < len(d.errs) && d.errs[i] != nil {
		return nil, d.errs[i]
	}
	if i < len(d.resps) {
		return d.resps[i], nil
	}
	return nil, errors.New("scripted: out of responses")
}

func TestTagsListReadBodyError(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &scriptedDoer{
		resps: []*http.Response{{StatusCode: 200, Header: http.Header{}, Body: errReadCloser{}}},
	}}
	_, err := c.TagsList(context.Background())
	if err == nil || !strings.Contains(err.Error(), "read tags body") {
		t.Fatalf("expected read-body error, got %v", err)
	}
}

func TestTagsListNextPageError(t *testing.T) {
	// A 200 page carrying a Link header with an unparseable target makes the loop
	// surface nextPageURL's error.
	body, _ := json.Marshal(tagsListResponse{Name: "repo", Tags: []string{"a"}})
	h := http.Header{}
	h.Set("Link", "<http://\x7f>; rel=\"next\"")
	c := &Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &scriptedDoer{
		resps: []*http.Response{{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(string(body)))}},
	}}
	_, err := c.TagsList(context.Background())
	if err == nil || !strings.Contains(err.Error(), "bad Link target") {
		t.Fatalf("expected next-page error, got %v", err)
	}
}

func TestResolveDigestTransportError(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo", HTTPClient: &scriptedDoer{
		errs: []error{errors.New("dial fail")},
	}}
	if _, err := c.resolveDigest(context.Background(), "v1"); err == nil {
		t.Fatal("expected transport error from resolveDigest")
	}
}
