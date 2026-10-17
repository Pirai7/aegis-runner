package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrCollision is returned when an idempotency key is reused with a different payload.
	ErrCollision = errors.New("idempotency key collision: payload hash does not match original submission")
)

// CanonicalizeAndHash generates a deterministic SHA-256 hash for any JSON-compatible payload
// by decoding and re-encoding with sorted keys.
func CanonicalizeAndHash(payload any) (string, []byte, error) {
	// First convert to standard raw JSON if not already bytes
	var rawBytes []byte
	switch p := payload.(type) {
	case []byte:
		rawBytes = p
	case string:
		rawBytes = []byte(p)
	default:
		var err error
		rawBytes, err = json.Marshal(payload)
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal payload: %w", err)
		}
	}

	// Unmarshal into generic interface to normalize numbers/maps
	var normalized any
	if err := json.Unmarshal(rawBytes, &normalized); err != nil {
		return "", nil, fmt.Errorf("invalid json payload: %w", err)
	}

	// Re-marshal canonically (Go's json.Marshal sorts map keys deterministically)
	canonicalBytes, err := json.Marshal(normalized)
	if err != nil {
		return "", nil, fmt.Errorf("failed to canonically marshal payload: %w", err)
	}

	hash := sha256.Sum256(canonicalBytes)
	return hex.EncodeToString(hash[:]), canonicalBytes, nil
}
