// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package registry

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrNotFound is the sentinel a 404 from the registry maps to. Test for it
	// with errors.Is so callers can distinguish "absent" from "transport error".
	ErrNotFound = errors.New("registry: not found")

	// ErrDigestMismatch is returned when pulled bytes do not hash to the
	// requested digest (corruption or a lying registry).
	ErrDigestMismatch = errors.New("registry: digest mismatch")

	// ErrBadDigest is returned for a syntactically invalid digest string.
	ErrBadDigest = errors.New("registry: malformed digest")

	// ErrNoUploadLocation is returned when an upload POST omits the Location
	// header the monolithic-upload flow needs.
	ErrNoUploadLocation = errors.New("registry: upload response missing Location header")

	// ErrNoAuthChallenge is returned when a 401 carries no usable
	// WWW-Authenticate Bearer challenge to satisfy.
	ErrNoAuthChallenge = errors.New("registry: 401 without a Bearer challenge")
)

// APIError is a typed registry error parsed from the OCI/Docker JSON error body
// (the {"errors":[{"code","message","detail"}]} envelope). It carries the HTTP
// status so callers can branch on it; a 404 additionally wraps ErrNotFound.
type APIError struct {
	StatusCode int
	Method     string
	URL        string
	Errors     []APIErrorDetail
	// Raw is the response body when it did not parse as the JSON envelope.
	Raw string
}

// APIErrorDetail is one entry of the registry error envelope.
type APIErrorDetail struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Detail  json.RawMessage `json:"detail,omitempty"`
}

func (e *APIError) Error() string {
	if len(e.Errors) > 0 {
		return fmt.Sprintf("registry: %s %s: %d: %s: %s",
			e.Method, e.URL, e.StatusCode, e.Errors[0].Code, e.Errors[0].Message)
	}
	if e.Raw != "" {
		return fmt.Sprintf("registry: %s %s: %d: %s", e.Method, e.URL, e.StatusCode, e.Raw)
	}
	return fmt.Sprintf("registry: %s %s: %d", e.Method, e.URL, e.StatusCode)
}

// Is lets errors.Is(err, ErrNotFound) succeed for a 404 APIError.
func (e *APIError) Is(target error) bool {
	return target == ErrNotFound && e.StatusCode == 404
}

// apiErrorEnvelope is the wire shape of a registry error body.
type apiErrorEnvelope struct {
	Errors []APIErrorDetail `json:"errors"`
}

// newAPIError builds an APIError from a status, request coordinates and the
// (already-read) response body, parsing the JSON envelope when present.
func newAPIError(method, url string, status int, body []byte) *APIError {
	e := &APIError{StatusCode: status, Method: method, URL: url}
	var env apiErrorEnvelope
	if err := json.Unmarshal(body, &env); err == nil && len(env.Errors) > 0 {
		e.Errors = env.Errors
	} else {
		e.Raw = string(body)
	}
	return e
}
