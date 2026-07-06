package mother

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientIPFromRequest(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		xRealIP    string
		want       string
	}{
		{
			name:       "remote addr without proxy headers",
			remoteAddr: "203.0.113.10:54321",
			want:       "203.0.113.10",
		},
		{
			name:       "x forwarded for first ip wins",
			remoteAddr: "10.0.0.1:1234",
			xff:        "198.51.100.7, 10.0.0.2",
			want:       "198.51.100.7",
		},
		{
			name:       "x real ip fallback",
			remoteAddr: "10.0.0.1:1234",
			xRealIP:    "198.51.100.8",
			want:       "198.51.100.8",
		},
		{
			name:       "ipv6 remote addr",
			remoteAddr: "[2001:db8::1]:54321",
			want:       "2001:db8::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/api/stream", nil)
			r.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				r.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.xRealIP != "" {
				r.Header.Set("X-Real-IP", tt.xRealIP)
			}

			got := clientIPFromRequest(r)
			if got != tt.want {
				t.Fatalf("clientIPFromRequest() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestChildIDFromIP(t *testing.T) {
	tests := map[string]string{
		"203.0.113.10": "ip_203_0_113_10",
		"2001:db8::1":  "ip_2001_db8__1",
		"":             "ip_unknown",
	}

	for input, want := range tests {
		if got := childIDFromIP(input); got != want {
			t.Fatalf("childIDFromIP(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestReapStaleChildrenRemovesStaleWSChild(t *testing.T) {
	now := time.Now()
	hub := &Hub{
		children: make(map[string]*ChildState),
		events:   make(chan interface{}, 10),
	}
	hub.children["ip_203_0_113_10"] = &ChildState{
		ID:            "ip_203_0_113_10",
		Transport:     "ws",
		LastHeartbeat: now.Add(-wsHeartbeatTimeout - time.Second),
	}

	hub.reapStaleChildrenOnce(now)

	if _, ok := hub.children["ip_203_0_113_10"]; ok {
		t.Fatal("stale ws child was not removed")
	}
}

func TestReapStaleChildrenKeepsFreshWSChild(t *testing.T) {
	now := time.Now()
	hub := &Hub{
		children: make(map[string]*ChildState),
		events:   make(chan interface{}, 10),
	}
	hub.children["ip_203_0_113_10"] = &ChildState{
		ID:            "ip_203_0_113_10",
		Transport:     "ws",
		LastHeartbeat: now.Add(-wsHeartbeatTimeout + time.Second),
	}

	hub.reapStaleChildrenOnce(now)

	if _, ok := hub.children["ip_203_0_113_10"]; !ok {
		t.Fatal("fresh ws child was removed")
	}
}

func TestReapStaleChildrenRemovesUnregisteredWSChild(t *testing.T) {
	now := time.Now()
	hub := &Hub{
		children: make(map[string]*ChildState),
		events:   make(chan interface{}, 10),
	}
	hub.children["ip_203_0_113_10"] = &ChildState{
		ID:          "ip_203_0_113_10",
		Transport:   "ws",
		ConnectedAt: now.Add(-wsHeartbeatTimeout - time.Second),
	}

	hub.reapStaleChildrenOnce(now)

	if _, ok := hub.children["ip_203_0_113_10"]; ok {
		t.Fatal("stale unregistered ws child was not removed")
	}
}

func TestDisconnectChildDoesNotRemoveReplacement(t *testing.T) {
	oldChild := &ChildState{ID: "ip_203_0_113_10", Transport: "ws"}
	newChild := &ChildState{ID: "ip_203_0_113_10", Transport: "ws"}
	hub := &Hub{
		children: map[string]*ChildState{"ip_203_0_113_10": newChild},
		events:   make(chan interface{}, 10),
	}

	if hub.disconnectChild("ip_203_0_113_10", oldChild) {
		t.Fatal("disconnectChild reported removing a replaced child")
	}
	if got := hub.children["ip_203_0_113_10"]; got != newChild {
		t.Fatal("disconnectChild removed the replacement child")
	}
}
