package child

import (
	"testing"
	"time"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

func TestLimitedBuffer(t *testing.T) {
	buf := newLimitedBuffer(5)

	n, err := buf.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("first write error: %v", err)
	}
	if n != 5 {
		t.Fatalf("first write length = %d, want 5", n)
	}
	if buf.String() != "hello" {
		t.Fatalf("buffer = %q, want hello", buf.String())
	}
	if buf.Truncated() {
		t.Fatal("buffer should not be truncated after exact-size write")
	}

	n, err = buf.Write([]byte(" world"))
	if err != nil {
		t.Fatalf("second write error: %v", err)
	}
	if n != 6 {
		t.Fatalf("second write length = %d, want 6", n)
	}
	if buf.String() != "hello" {
		t.Fatalf("buffer = %q, want hello", buf.String())
	}
	if !buf.Truncated() {
		t.Fatal("buffer should be marked truncated")
	}
}

func TestRegisteredResetsReconnectBackoff(t *testing.T) {
	agent := NewAgent("ws://example.invalid/api/stream", "", "test")
	agent.reconnect = 32 * time.Second

	agent.handleMessage(&protocol.Message{
		Type: protocol.TypeRegistered,
		Payload: map[string]interface{}{
			"child_id": "ip_203_0_113_10",
		},
	})

	if agent.childID != "ip_203_0_113_10" {
		t.Fatalf("childID = %q, want ip_203_0_113_10", agent.childID)
	}
	if agent.reconnect != time.Second {
		t.Fatalf("reconnect = %v, want 1s", agent.reconnect)
	}
}
