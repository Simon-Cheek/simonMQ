package payload_test

import (
	"testing"

	"dist-mq/e2e-tests/payload"
)

func TestRoundTripAtEverySize(t *testing.T) {
	for _, size := range []int{0, 1, 8, 64, 4096} {
		body := payload.Encode("tok-42", size)
		if got := payload.Token(body); got != "tok-42" {
			t.Errorf("size %d: token = %q, want tok-42", size, got)
		}
		if len(body) < size {
			t.Errorf("size %d: body is %d bytes, want at least %d", size, len(body), size)
		}
	}
}

// Padding is a benchmark knob; identity is not. A size too small to hold the
// token has to lose the padding, never the token.
func TestTokenSurvivesUndersizedPayload(t *testing.T) {
	body := payload.Encode("a-very-long-token", 4)
	if got := payload.Token(body); got != "a-very-long-token" {
		t.Fatalf("token = %q, want a-very-long-token", got)
	}
}

func TestBareBodyIsItsOwnToken(t *testing.T) {
	if got := payload.Token([]byte("hand-written")); got != "hand-written" {
		t.Fatalf("token = %q, want hand-written", got)
	}
}

func TestPaddingIsNotMistakenForToken(t *testing.T) {
	body := payload.Encode("t", 1024)
	if got := payload.Token(body); got != "t" {
		t.Fatalf("token = %q, want t", got)
	}
}
