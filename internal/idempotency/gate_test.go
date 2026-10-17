package idempotency

import (
	"testing"
)

func TestCanonicalizeAndHash(t *testing.T) {
	// Two JSON strings with identical key-value pairs but different whitespace and key order
	jsonA := `{"b": 2, "a": "hello", "nested": {"z": 10, "y": 20}}`
	jsonB := `{"nested": {"y": 20, "z": 10}, "a": "hello", "b": 2}`

	hashA, canonicalA, err := CanonicalizeAndHash(jsonA)
	if err != nil {
		t.Fatalf("unexpected error hashing jsonA: %v", err)
	}

	hashB, canonicalB, err := CanonicalizeAndHash(jsonB)
	if err != nil {
		t.Fatalf("unexpected error hashing jsonB: %v", err)
	}

	if hashA != hashB {
		t.Errorf("expected hashes to match, got %s != %s", hashA, hashB)
	}

	if string(canonicalA) != string(canonicalB) {
		t.Errorf("expected canonical json to match, got %s != %s", string(canonicalA), string(canonicalB))
	}

	// Payload with different value should yield different hash
	jsonC := `{"b": 3, "a": "hello", "nested": {"z": 10, "y": 20}}`
	hashC, _, err := CanonicalizeAndHash(jsonC)
	if err != nil {
		t.Fatalf("unexpected error hashing jsonC: %v", err)
	}

	if hashA == hashC {
		t.Errorf("expected different hashes for different payloads, got collision: %s", hashA)
	}
}
