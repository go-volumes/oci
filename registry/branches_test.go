// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package registry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// doerFunc adapts a function to httpDoer.
type doerFunc func(*http.Request) (*http.Response, error)

func (d doerFunc) Do(r *http.Request) (*http.Response, error) { return d(r) }

// mkResp builds a minimal *http.Response.
func mkResp(status int, body string, hdr http.Header) *http.Response {
	if hdr == nil {
		hdr = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Header:     hdr,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestDefaultDoerUsed(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo"}
	if c.doer() != http.DefaultClient {
		t.Fatal("expected http.DefaultClient default")
	}
}

func TestTransportError(t *testing.T) {
	want := errors.New("network down")
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) { return nil, want })}
	ctx := context.Background()
	if _, err := c.BlobExists(ctx, Digest([]byte("a"))); !errors.Is(err, want) {
		t.Errorf("BlobExists transport err: %v", err)
	}
	if _, err := c.PullBlob(ctx, Digest([]byte("a"))); !errors.Is(err, want) {
		t.Errorf("PullBlob transport err: %v", err)
	}
	if _, err := c.PushBlob(ctx, []byte("a")); !errors.Is(err, want) {
		t.Errorf("PushBlob transport err: %v", err)
	}
	if _, _, err := c.GetManifest(ctx, "v1"); !errors.Is(err, want) {
		t.Errorf("GetManifest transport err: %v", err)
	}
	if _, err := c.PutManifest(ctx, "v1", "mt", []byte("b")); !errors.Is(err, want) {
		t.Errorf("PutManifest transport err: %v", err)
	}
}

func TestBlobExistsServerError(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return mkResp(500, `{"errors":[{"code":"X"}]}`, nil), nil
		})}
	var ae *APIError
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); !errors.As(err, &ae) {
		t.Fatalf("expected APIError, got %v", err)
	}
}

func TestPushBlobUploadStartFails(t *testing.T) {
	// HEAD says absent, POST upload-start returns 500.
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			switch r.Method {
			case http.MethodHead:
				return mkResp(404, "", nil), nil
			case http.MethodPost:
				return mkResp(500, `{"errors":[{"code":"NOPE"}]}`, nil), nil
			}
			return mkResp(500, "", nil), nil
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); err == nil {
		t.Fatal("expected upload-start failure")
	}
}

func TestPushBlobMissingLocation(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			switch r.Method {
			case http.MethodHead:
				return mkResp(404, "", nil), nil
			case http.MethodPost:
				return mkResp(202, "", nil), nil // no Location
			}
			return mkResp(500, "", nil), nil
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); !errors.Is(err, ErrNoUploadLocation) {
		t.Fatalf("expected ErrNoUploadLocation, got %v", err)
	}
}

func TestPushBlobBadLocation(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			switch r.Method {
			case http.MethodHead:
				return mkResp(404, "", nil), nil
			case http.MethodPost:
				h := http.Header{"Location": {"http://[::1]:namedport/x"}}
				return mkResp(202, "", h), nil
			}
			return mkResp(500, "", nil), nil
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); err == nil {
		t.Fatal("expected bad-location parse error")
	}
}

func TestPushBlobPutFails(t *testing.T) {
	c := &Client{BaseURL: "http://reg.example", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			switch r.Method {
			case http.MethodHead:
				return mkResp(404, "", nil), nil
			case http.MethodPost:
				return mkResp(202, "", http.Header{"Location": {"/v2/repo/up/1"}}), nil
			case http.MethodPut:
				return mkResp(400, `{"errors":[{"code":"DIGEST_INVALID"}]}`, nil), nil
			}
			return mkResp(500, "", nil), nil
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); err == nil {
		t.Fatal("expected PUT failure")
	}
}

func TestPushBlobExistsSkipsUpload(t *testing.T) {
	posts := 0
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodHead {
				return mkResp(200, "", nil), nil // already present
			}
			if r.Method == http.MethodPost {
				posts++
			}
			return mkResp(201, "", nil), nil
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if posts != 0 {
		t.Fatalf("upload POST issued despite existing blob (%d)", posts)
	}
}

func TestPutManifestFails(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return mkResp(400, `{"errors":[{"code":"MANIFEST_INVALID"}]}`, nil), nil
		})}
	if _, err := c.PutManifest(context.Background(), "v1", "mt", []byte("b")); err == nil {
		t.Fatal("expected manifest PUT failure")
	}
}

func TestPutManifestComputesDigestWithoutHeader(t *testing.T) {
	body := []byte("manifest-body")
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return mkResp(201, "", nil), nil // no Docker-Content-Digest
		})}
	dg, err := c.PutManifest(context.Background(), "v1", "mt", body)
	if err != nil {
		t.Fatal(err)
	}
	if dg != Digest(body) {
		t.Fatalf("computed digest %s != %s", dg, Digest(body))
	}
}

func TestPullBlobBodyReadError(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{},
				Body: io.NopCloser(errReader{})}, nil
		})}
	if _, err := c.PullBlob(context.Background(), Digest([]byte("a"))); err == nil {
		t.Fatal("expected body read error")
	}
}

func TestGetManifestBodyReadError(t *testing.T) {
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{},
				Body: io.NopCloser(errReader{})}, nil
		})}
	if _, _, err := c.GetManifest(context.Background(), "v1"); err == nil {
		t.Fatal("expected manifest body read error")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read boom") }

func TestAuthNoBearerChallengeAnonymous(t *testing.T) {
	// Anonymous client, 401 without WWW-Authenticate -> ErrNoAuthChallenge.
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return mkResp(401, "", nil), nil
		})}
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); !errors.Is(err, ErrNoAuthChallenge) {
		t.Fatalf("expected ErrNoAuthChallenge, got %v", err)
	}
}

func TestAuthBasicRealm401(t *testing.T) {
	// Client with creds but a non-bearer 401 -> typed Unauthorized APIError.
	c := &Client{BaseURL: "http://x", Repository: "repo", Username: "u", Password: "p",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) {
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Basic realm="x"`}}), nil
		})}
	var ae *APIError
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); !errors.As(err, &ae) || ae.StatusCode != 401 {
		t.Fatalf("expected 401 APIError, got %v", err)
	}
}

func TestAuthTokenEndpointFails(t *testing.T) {
	// 401 bearer challenge, but token endpoint returns 500.
	step := 0
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/token") {
				return mkResp(500, `{"errors":[{"code":"AUTH"}]}`, nil), nil
			}
			step++
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://reg/token",service="s"`}}), nil
		})}
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); err == nil {
		t.Fatal("expected token-endpoint failure")
	}
}

func TestAuthTokenNoRealm(t *testing.T) {
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer service="s"`}}), nil
		})}
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); !errors.Is(err, ErrNoAuthChallenge) {
		t.Fatalf("expected ErrNoAuthChallenge (no realm), got %v", err)
	}
}

func TestAuthTokenBadRealmURL(t *testing.T) {
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://[::1]:bad",service="s"`}}), nil
		})}
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); err == nil {
		t.Fatal("expected bad realm URL error")
	}
}

func TestAuthTokenEmptyToken(t *testing.T) {
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/token") {
				return mkResp(200, `{}`, nil), nil // neither token nor access_token
			}
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://reg/token"`}}), nil
		})}
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); !errors.Is(err, ErrNoAuthChallenge) {
		t.Fatalf("expected ErrNoAuthChallenge (empty token), got %v", err)
	}
}

func TestAuthTokenDecodeError(t *testing.T) {
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/token") {
				return mkResp(200, `{not json`, nil), nil
			}
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://reg/token"`}}), nil
		})}
	if _, err := c.BlobExists(context.Background(), Digest([]byte("a"))); err == nil {
		t.Fatal("expected token decode error")
	}
}

func TestAuthAccessTokenField(t *testing.T) {
	// Token endpoint uses "access_token" instead of "token".
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/token") {
				return mkResp(200, `{"access_token":"AT"}`, nil), nil
			}
			if r.Header.Get("Authorization") == "Bearer AT" {
				return mkResp(200, "", nil), nil
			}
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://reg/token"`}}), nil
		})}
	ok, err := c.BlobExists(context.Background(), Digest([]byte("a")))
	if err != nil || !ok {
		t.Fatalf("access_token path: ok=%v err=%v", ok, err)
	}
}

func TestAuthRetryRewindsBody(t *testing.T) {
	// A bearer 401 on the PUT step must rewind the body so the retry resends it.
	var gotBodies []string
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/token") {
				return mkResp(200, `{"token":"T"}`, nil), nil
			}
			if r.Method == http.MethodPut {
				b, _ := io.ReadAll(r.Body)
				gotBodies = append(gotBodies, string(b))
				if r.Header.Get("Authorization") != "Bearer T" {
					return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://reg/token"`}}), nil
				}
				return mkResp(201, "", nil), nil
			}
			return mkResp(500, "", nil), nil
		})}
	if _, err := c.PutManifest(context.Background(), "v1", "mt", []byte("BODY")); err != nil {
		t.Fatal(err)
	}
	if len(gotBodies) != 2 || gotBodies[0] != "BODY" || gotBodies[1] != "BODY" {
		t.Fatalf("body not rewound across retry: %v", gotBodies)
	}
}

func TestParseChallengeQuoting(t *testing.T) {
	p := parseChallenge(`Bearer realm="https://a/token",service="reg,with,commas",scope="repository:r:pull"`)
	if p["service"] != "reg,with,commas" {
		t.Fatalf("quoted comma not preserved: %q", p["service"])
	}
	if p["scope"] != "repository:r:pull" {
		t.Fatalf("scope: %q", p["scope"])
	}
	// A bare token with no '=' is ignored without panicking.
	_ = parseChallenge(`Bearer realm="x", junk`)
}

func TestV2URLNoDoubleSlash(t *testing.T) {
	c := &Client{BaseURL: "http://reg/", Repository: "repo"}
	if got := c.v2url("blobs/x"); got != "http://reg/v2/repo/blobs/x" {
		t.Fatalf("v2url trailing-slash handling: %s", got)
	}
}

func TestAppendDigestQueryBadBase(t *testing.T) {
	if _, err := appendDigestQuery("http://[::1]:bad", "/loc", "sha256:x"); err == nil {
		t.Fatal("expected bad base URL error")
	}
}

func TestDrainCloseNil(t *testing.T) { drainClose(nil) } // no panic

func TestPushBlobPostTransportError(t *testing.T) {
	// HEAD 404 (absent), then POST fails at the transport layer.
	boom := errors.New("post boom")
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodHead {
				return mkResp(404, "", nil), nil
			}
			return nil, boom
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); !errors.Is(err, boom) {
		t.Fatalf("expected POST transport error, got %v", err)
	}
}

func TestPushBlobPutTransportError(t *testing.T) {
	boom := errors.New("put boom")
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			switch r.Method {
			case http.MethodHead:
				return mkResp(404, "", nil), nil
			case http.MethodPost:
				return mkResp(202, "", http.Header{"Location": {"/v2/repo/up/1"}}), nil
			case http.MethodPut:
				return nil, boom
			}
			return mkResp(500, "", nil), nil
		})}
	if _, err := c.PushBlob(context.Background(), []byte("a")); !errors.Is(err, boom) {
		t.Fatalf("expected PUT transport error, got %v", err)
	}
}

// badSeekReader reads fine but fails to Seek, exercising the auth-retry rewind
// error branch.
type badSeekReader struct{ r *strings.Reader }

func (b *badSeekReader) Read(p []byte) (int, error)     { return b.r.Read(p) }
func (b *badSeekReader) Seek(int64, int) (int64, error) { return 0, errors.New("seek boom") }

func TestAuthRewindSeekError(t *testing.T) {
	c := &Client{BaseURL: "http://reg", Repository: "repo",
		HTTPClient: doerFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/token") {
				return mkResp(200, `{"token":"T"}`, nil), nil
			}
			return mkResp(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://reg/token"`}}), nil
		})}
	body := &badSeekReader{r: strings.NewReader("BODY")}
	_, err := c.do(context.Background(), http.MethodPut, "http://reg/v2/repo/manifests/v1", body, nil)
	if err == nil || !strings.Contains(err.Error(), "rewind") {
		t.Fatalf("expected rewind seek error, got %v", err)
	}
}

func TestAttemptBadMethod(t *testing.T) {
	// An invalid method makes http.NewRequestWithContext fail.
	c := &Client{BaseURL: "http://x", Repository: "repo",
		HTTPClient: doerFunc(func(*http.Request) (*http.Response, error) { return mkResp(200, "", nil), nil })}
	if _, err := c.attempt(context.Background(), "BAD\nMETHOD", "http://x", nil, nil, false); err == nil {
		t.Fatal("expected bad-method request build error")
	}
}

func TestFetchTokenBadRequestBuild(t *testing.T) {
	c := &Client{}
	// A realm with a control character in it makes the token GET request fail to
	// build after URL parsing succeeds is hard; instead drive a transport error.
	_, err := c.fetchToken(context.Background(),
		`Bearer realm="http://127.0.0.1:0/token"`)
	if err == nil {
		t.Fatal("expected token request transport error")
	}
}

func TestVerifyDigestBadWant(t *testing.T) {
	if err := verifyDigest("not-a-digest", []byte("x")); !errors.Is(err, ErrBadDigest) {
		t.Fatalf("expected ErrBadDigest, got %v", err)
	}
}

func TestValidateDigestBadHex(t *testing.T) {
	// 64 chars but not valid hex -> hex.DecodeString error branch.
	bad := "sha256:" + strings.Repeat("zz", 32)
	if err := validateDigest(bad); !errors.Is(err, ErrBadDigest) {
		t.Fatalf("expected ErrBadDigest for non-hex, got %v", err)
	}
}
