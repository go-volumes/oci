// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// do issues an HTTP request, applying any cached bearer token and Basic creds,
// and transparently performing the Docker/OCI bearer-token handshake on a 401:
// it parses the WWW-Authenticate challenge, fetches a token, and retries once.
// The body, when non-nil, must be re-readable across the retry, so callers pass
// an io.ReadSeeker (bytes.Reader); nil bodies are fine.
func (c *Client) do(ctx context.Context, method, u string, body io.Reader, hdr http.Header) (*http.Response, error) {
	resp, err := c.attempt(ctx, method, u, body, hdr, true)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	// 401: try the bearer-token handshake, then retry once.
	challenge := resp.Header.Get("Www-Authenticate")
	drainClose(resp.Body)
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		// No bearer challenge we can satisfy. When the client carries creds the
		// registry simply rejected them (e.g. a Basic-only realm): surface the
		// 401 as a typed error. With no creds and no usable challenge there is
		// nothing to try, so report ErrNoAuthChallenge.
		if c.Username != "" || c.Password != "" {
			return nil, &APIError{StatusCode: http.StatusUnauthorized, Method: method, URL: u,
				Raw: "unauthorized"}
		}
		return nil, ErrNoAuthChallenge
	}
	token, err := c.fetchToken(ctx, challenge)
	if err != nil {
		return nil, err
	}
	c.token = token
	// Rewind a seekable body (bytes.Reader) so the retry resends it intact.
	if s, ok := body.(io.Seeker); ok {
		if _, err := s.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("registry: rewind body for auth retry: %w", err)
		}
	}
	return c.attempt(ctx, method, u, body, hdr, true)
}

// attempt performs a single HTTP request with the current auth state applied.
func (c *Client) attempt(ctx context.Context, method, u string, body io.Reader, hdr http.Header, auth bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, fmt.Errorf("registry: build request: %w", err)
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if auth {
		switch {
		case c.token != "":
			req.Header.Set("Authorization", "Bearer "+c.token)
		case c.Username != "" || c.Password != "":
			req.SetBasicAuth(c.Username, c.Password)
		}
	}
	resp, err := c.doer().Do(req)
	if err != nil {
		return nil, fmt.Errorf("registry: %s %s: %w", method, u, err)
	}
	return resp, nil
}

// tokenResponse is the token endpoint reply; registries use either field.
type tokenResponse struct {
	Token       string `json:"token"`
	AccessToken string `json:"access_token"`
}

// fetchToken parses a "Bearer realm=...,service=...,scope=..." challenge, GETs
// the token endpoint (with Basic creds when configured), and returns the token.
func (c *Client) fetchToken(ctx context.Context, challenge string) (string, error) {
	params := parseChallenge(challenge)
	realm := params["realm"]
	if realm == "" {
		return "", fmt.Errorf("%w: challenge %q has no realm", ErrNoAuthChallenge, challenge)
	}
	tu, err := url.Parse(realm)
	if err != nil {
		return "", fmt.Errorf("registry: bad token realm %q: %w", realm, err)
	}
	q := tu.Query()
	if s := params["service"]; s != "" {
		q.Set("service", s)
	}
	if s := params["scope"]; s != "" {
		q.Set("scope", s)
	}
	tu.RawQuery = q.Encode()

	// Build the request directly from the already-parsed, already-validated URL,
	// so there is no second parse step that could fail.
	req := (&http.Request{
		Method: http.MethodGet,
		URL:    tu,
		Header: http.Header{},
		Host:   tu.Host,
	}).WithContext(ctx)
	if c.Username != "" || c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	resp, err := c.doer().Do(req)
	if err != nil {
		return "", fmt.Errorf("registry: token request: %w", err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", c.errorFrom(http.MethodGet, tu.String(), resp)
	}
	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("registry: decode token: %w", err)
	}
	if tr.Token != "" {
		return tr.Token, nil
	}
	if tr.AccessToken != "" {
		return tr.AccessToken, nil
	}
	return "", fmt.Errorf("%w: token endpoint returned no token", ErrNoAuthChallenge)
}

// parseChallenge parses the comma-separated key="value" pairs after "Bearer ".
func parseChallenge(challenge string) map[string]string {
	out := map[string]string{}
	rest := strings.TrimSpace(challenge[len("Bearer "):])
	for _, part := range splitChallenge(rest) {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"`)
	}
	return out
}

// splitChallenge splits on commas that are not inside double quotes.
func splitChallenge(s string) []string {
	var parts []string
	var cur strings.Builder
	inQuote := false
	for _, r := range s {
		switch r {
		case '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case ',':
			if inQuote {
				cur.WriteRune(r)
			} else {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}
