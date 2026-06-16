// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

// Package registry is a minimal, dependency-free OCI Distribution v2 client
// built on net/http. It speaks just enough of the spec to push and pull blobs
// and manifests for content-addressed image artifacts: monolithic blob upload,
// HEAD-based deduplication, manifest get/put, and the three auth modes a real
// registry needs (anonymous, HTTP Basic, and the Docker/OCI bearer-token
// handshake). Digests are sha256 and every pulled blob is verified.
package registry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// httpDoer is the slice of *http.Client the registry depends on, injectable so
// tests can drive the client without a network and the bearer-token flow can be
// exercised deterministically.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Client talks to one repository on one OCI Distribution v2 registry.
type Client struct {
	// BaseURL is the registry root, e.g. "https://registry.example.com". No
	// trailing slash is required.
	BaseURL string
	// Repository is the repository name (namespace/name), e.g. "library/alpine".
	Repository string
	// Username and Password, when set, enable HTTP Basic auth and are also the
	// credentials presented to a bearer-token endpoint during the OCI auth
	// handshake. Leave empty for anonymous access.
	Username string
	Password string
	// HTTPClient performs requests; it defaults to http.DefaultClient.
	HTTPClient httpDoer

	// token caches the most recently obtained bearer token for reuse.
	token string
}

// doer returns the configured HTTP client or the default.
func (c *Client) doer() httpDoer {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// v2url builds an absolute /v2/<repo>/<suffix> URL.
func (c *Client) v2url(suffix string) string {
	return fmt.Sprintf("%s/v2/%s/%s", strings.TrimRight(c.BaseURL, "/"), c.Repository, suffix)
}

// MediaTypeManifest is the OCI image manifest media type.
const MediaTypeManifest = "application/vnd.oci.image.manifest.v1+json"

// BlobExists reports whether a blob with the given digest is already present in
// the repository (HEAD /v2/<repo>/blobs/<digest>). It powers push-time dedup.
func (c *Client) BlobExists(ctx context.Context, digest string) (bool, error) {
	if err := validateDigest(digest); err != nil {
		return false, err
	}
	resp, err := c.do(ctx, http.MethodHead, c.v2url("blobs/"+digest), nil, nil)
	if err != nil {
		return false, err
	}
	defer drainClose(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, c.errorFrom(http.MethodHead, c.v2url("blobs/"+digest), resp)
	}
}

// PullBlob fetches a blob by digest and verifies the returned bytes hash to it.
func (c *Client) PullBlob(ctx context.Context, digest string) ([]byte, error) {
	if err := validateDigest(digest); err != nil {
		return nil, err
	}
	u := c.v2url("blobs/" + digest)
	resp, err := c.do(ctx, http.MethodGet, u, nil, nil)
	if err != nil {
		return nil, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, c.errorFrom(http.MethodGet, u, resp)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("registry: read blob body: %w", err)
	}
	if err := verifyDigest(digest, data); err != nil {
		return nil, err
	}
	return data, nil
}

// PushBlob uploads data via the monolithic two-step flow (POST an upload
// session, PUT the bytes with the digest query parameter) and returns its
// digest. If the blob already exists (BlobExists), the upload is skipped.
func (c *Client) PushBlob(ctx context.Context, data []byte) (string, error) {
	digest := Digest(data)

	exists, err := c.BlobExists(ctx, digest)
	if err != nil {
		return "", err
	}
	if exists {
		return digest, nil
	}

	// Step 1: open an upload session.
	startURL := c.v2url("blobs/uploads/")
	resp, err := c.do(ctx, http.MethodPost, startURL, nil, nil)
	if err != nil {
		return "", err
	}
	loc := resp.Header.Get("Location")
	drainClose(resp.Body)
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusCreated {
		return "", c.errorFrom(http.MethodPost, startURL, resp)
	}
	if loc == "" {
		return "", ErrNoUploadLocation
	}

	// Step 2: PUT the bytes with ?digest=... appended to the upload location.
	putURL, err := appendDigestQuery(c.BaseURL, loc, digest)
	if err != nil {
		return "", err
	}
	hdr := http.Header{"Content-Type": {"application/octet-stream"}}
	resp2, err := c.do(ctx, http.MethodPut, putURL, bytes.NewReader(data), hdr)
	if err != nil {
		return "", err
	}
	defer drainClose(resp2.Body)
	if resp2.StatusCode != http.StatusCreated && resp2.StatusCode != http.StatusOK {
		return "", c.errorFrom(http.MethodPut, putURL, resp2)
	}
	return digest, nil
}

// GetManifest fetches the manifest tagged or digest-referenced by ref and
// returns its Content-Type and body.
func (c *Client) GetManifest(ctx context.Context, ref string) (string, []byte, error) {
	u := c.v2url("manifests/" + ref)
	hdr := http.Header{"Accept": {MediaTypeManifest}}
	resp, err := c.do(ctx, http.MethodGet, u, nil, hdr)
	if err != nil {
		return "", nil, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", nil, c.errorFrom(http.MethodGet, u, resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, fmt.Errorf("registry: read manifest body: %w", err)
	}
	return resp.Header.Get("Content-Type"), body, nil
}

// PutManifest stores body under ref with the given media type and returns the
// manifest digest (from the registry's Docker-Content-Digest header when given,
// otherwise computed locally).
func (c *Client) PutManifest(ctx context.Context, ref, mediaType string, body []byte) (string, error) {
	u := c.v2url("manifests/" + ref)
	hdr := http.Header{"Content-Type": {mediaType}}
	resp, err := c.do(ctx, http.MethodPut, u, bytes.NewReader(body), hdr)
	if err != nil {
		return "", err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", c.errorFrom(http.MethodPut, u, resp)
	}
	if d := resp.Header.Get("Docker-Content-Digest"); d != "" {
		return d, nil
	}
	return Digest(body), nil
}

// errorFrom reads resp's body and builds the typed APIError for a failed call.
func (c *Client) errorFrom(method, u string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return newAPIError(method, u, resp.StatusCode, body)
}

// drainClose drains and closes a response body so the connection can be reused.
func drainClose(rc io.ReadCloser) {
	if rc == nil {
		return
	}
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
}

// appendDigestQuery appends ?digest=<digest> to the (possibly relative) upload
// location, resolving it against the registry base URL.
func appendDigestQuery(base, location, digest string) (string, error) {
	bu, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("registry: bad base URL %q: %w", base, err)
	}
	lu, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("registry: bad upload location %q: %w", location, err)
	}
	resolved := bu.ResolveReference(lu)
	q := resolved.Query()
	q.Set("digest", digest)
	resolved.RawQuery = q.Encode()
	return resolved.String(), nil
}
