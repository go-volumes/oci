// Copyright (c) 2026, go-volumes
// SPDX-License-Identifier: BSD-3-Clause

package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Digest computes the canonical content digest of data as "sha256:<lowerhex>",
// the form OCI registries and manifests use to address blobs.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// verifyDigest checks that data hashes to want, returning ErrDigestMismatch
// otherwise. want must be a well-formed "sha256:<hex>" digest.
func verifyDigest(want string, data []byte) error {
	if err := validateDigest(want); err != nil {
		return err
	}
	got := Digest(data)
	if got != want {
		return fmt.Errorf("%w: want %s got %s", ErrDigestMismatch, want, got)
	}
	return nil
}

// validateDigest checks that s is a syntactically valid sha256 digest.
func validateDigest(s string) error {
	const prefix = "sha256:"
	hexPart, ok := strings.CutPrefix(s, prefix)
	if !ok {
		return fmt.Errorf("%w: %q missing %q prefix", ErrBadDigest, s, prefix)
	}
	if len(hexPart) != sha256.Size*2 {
		return fmt.Errorf("%w: %q has %d hex chars, want %d", ErrBadDigest, s, len(hexPart), sha256.Size*2)
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return fmt.Errorf("%w: %q: %v", ErrBadDigest, s, err)
	}
	return nil
}
