package mother

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

// HTTP child support: pure HTTP long-poll transport with the same message
// semantics as the WebSocket child. It is intentionally small: mother queues
// protocol.Message values per child; child polls commands and POSTs results.
type httpOutbound struct {
	msg protocol.Message
}

func (h *Hub) ensureHTTPQueues() {
	h.httpMu.Lock()
	if h.httpQueues == nil {
		h.httpQueues = make(map[string]chan protocol.Message)
	}
	h.httpMu.Unlock()
}

func (h *Hub) httpQueue(childID string) (chan protocol.Message, bool) {
	h.httpMu.RLock()
	q, ok := h.httpQueues[childID]
	h.httpMu.RUnlock()
	return q, ok
}

func (h *Hub) createHTTPChild(remoteAddr string, reg protocol.RegisterPayload) *ChildState {
	h.ensureHTTPQueues()
	clientIP := cleanClientIP(remoteAddr)
	if clientIP == "" {
		clientIP = "unknown"
	}
	childID := childIDFromIP(clientIP)
	child := &ChildState{
		ID:            childID,
		Hostname:      reg.Hostname,
		OS:            reg.OS,
		Arch:          reg.Arch,
		Version:       reg.Version,
		RemoteAddr:    clientIP,
		Transport:     "http",
		ConnectedAt:   time.Now(),
		LastHeartbeat: time.Now(),
	}

	replaced := h.replaceChild(child)

	h.httpMu.Lock()
	h.httpQueues[childID] = make(chan protocol.Message, 256)
	h.httpMu.Unlock()

	if replaced {
		log.Printf("[hub] http child %s reconnected: %s (%s/%s)", child.ID, reg.Hostname, reg.OS, reg.Arch)
	} else {
		log.Printf("[hub] http child %s registered: %s (%s/%s)", child.ID, reg.Hostname, reg.OS, reg.Arch)
	}
	h.broadcastEvent("child_registered", map[string]interface{}{
		"id": child.ID, "hostname": reg.Hostname, "os": reg.OS, "arch": reg.Arch, "transport": "http",
	})
	return child
}

func (h *Hub) removeHTTPChild(childID string, expected ...*ChildState) {
	var child *ChildState
	h.mu.Lock()
	child = h.children[childID]
	if child == nil || child.Transport != "http" || (len(expected) > 0 && expected[0] != nil && child != expected[0]) {
		h.mu.Unlock()
		return
	}
	delete(h.children, childID)
	h.mu.Unlock()

	h.closeHTTPQueue(childID)
	h.broadcastEvent("child_disconnected", childID)
}

func (h *Hub) closeHTTPQueue(childID string) {
	h.httpMu.Lock()
	if q, ok := h.httpQueues[childID]; ok {
		close(q)
		delete(h.httpQueues, childID)
	}
	h.httpMu.Unlock()
}

func (h *Hub) sendHTTP(child *ChildState, msg protocol.Message) error {
	h.ensureHTTPQueues()
	q, ok := h.httpQueue(child.ID)
	if !ok {
		return ErrChildNotFound
	}
	select {
	case q <- msg:
		return nil
	default:
		return fmt.Errorf("http child queue full")
	}
}

func (h *Hub) handleHTTPMessage(child *ChildState, msg *protocol.Message) {
	msg.ChildID = child.ID
	h.handleMessage(child, msg)
}

type httpChildRegisterRequest struct {
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Version  string `json:"version"`
}

type httpChildPollResponse struct {
	Messages []protocol.Message `json:"messages"`
}

func remoteAddr(r *http.Request) string {
	return clientIPFromRequest(r)
}

func (h *Hub) requireHTTPChild(w http.ResponseWriter, r *http.Request) (*ChildState, bool) {
	childID := r.URL.Query().Get("id")
	if childID == "" {
		childID = r.Header.Get("X-Child-ID")
	}
	if childID == "" {
		writeJSON(w, map[string]string{"error": "missing child id"})
		return nil, false
	}
	h.mu.RLock()
	child, ok := h.children[childID]
	h.mu.RUnlock()
	if !ok || child.Transport != "http" {
		writeJSON(w, map[string]string{"error": "child not found"})
		return nil, false
	}
	return child, true
}

// SetupHTTPChildRoutes registers the HTTP long-poll control transport.
func SetupHTTPChildRoutes(mux *http.ServeMux, hub *Hub) {
	// Register a new HTTP child. PSK is query ?key= or X-Child-Key.
	mux.HandleFunc("/api/http/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		if hub.psk != "" && r.URL.Query().Get("key") != hub.psk && r.Header.Get("X-Child-Key") != hub.psk {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req httpChildRegisterRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, map[string]string{"error": err.Error()})
			return
		}
		child := hub.createHTTPChild(remoteAddr(r), protocol.RegisterPayload{
			Hostname: req.Hostname,
			OS:       req.OS,
			Arch:     req.Arch,
			Version:  req.Version,
		})
		writeJSON(w, map[string]interface{}{"child_id": child.ID, "heartbeat_s": 5})
	})

	// Long poll for queued mother->child messages. Returns [] immediately if timeout.
	mux.HandleFunc("/api/http/poll", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", 405)
			return
		}
		child, ok := hub.requireHTTPChild(w, r)
		if !ok {
			return
		}
		child.mu.Lock()
		child.LastHeartbeat = time.Now()
		child.mu.Unlock()

		q, ok := hub.httpQueue(child.ID)
		if !ok {
			writeJSON(w, httpChildPollResponse{Messages: nil})
			return
		}

		timeout := 25 * time.Second
		if t := r.URL.Query().Get("timeout"); t != "" {
			if d, err := time.ParseDuration(t + "s"); err == nil && d > 0 && d <= 60*time.Second {
				timeout = d
			}
		}

		var msgs []protocol.Message
		select {
		case msg, ok := <-q:
			if ok {
				msgs = append(msgs, msg)
			} else {
				writeJSON(w, httpChildPollResponse{Messages: msgs})
				return
			}
		case <-time.After(timeout):
		}
		for len(msgs) < 32 {
			select {
			case msg, ok := <-q:
				if ok {
					msgs = append(msgs, msg)
				} else {
					writeJSON(w, httpChildPollResponse{Messages: msgs})
					return
				}
			default:
				writeJSON(w, httpChildPollResponse{Messages: msgs})
				return
			}
		}
		writeJSON(w, httpChildPollResponse{Messages: msgs})
	})

	// Child posts any protocol messages back: task_result/report/tunnel_ready/tunnel_data.
	mux.HandleFunc("/api/http/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		child, ok := hub.requireHTTPChild(w, r)
		if !ok {
			return
		}
		var req struct {
			Messages []protocol.Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, map[string]string{"error": err.Error()})
			return
		}
		for i := range req.Messages {
			hub.handleHTTPMessage(child, &req.Messages[i])
		}
		writeJSON(w, map[string]interface{}{"ok": true, "count": len(req.Messages)})
	})

	// Legacy/easy result endpoint: POST one task_result directly.
	mux.HandleFunc("/api/http/result", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		child, ok := hub.requireHTTPChild(w, r)
		if !ok {
			return
		}
		var payload protocol.TaskResultPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, map[string]string{"error": err.Error()})
			return
		}
		msg := protocol.NewMessage(protocol.TypeTaskResult, payload)
		hub.handleHTTPMessage(child, &msg)
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("/api/http/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		child, ok := hub.requireHTTPChild(w, r)
		if !ok {
			return
		}
		child.mu.Lock()
		child.LastHeartbeat = time.Now()
		child.mu.Unlock()
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("/api/http/disconnect", func(w http.ResponseWriter, r *http.Request) {
		child, ok := hub.requireHTTPChild(w, r)
		if !ok {
			return
		}
		hub.removeHTTPChild(child.ID)
		writeJSON(w, map[string]bool{"ok": true})
	})

	// Tiny helper for raw tunnel_data when writing minimal clients.
	mux.HandleFunc("/api/http/tunnel_data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		child, ok := hub.requireHTTPChild(w, r)
		if !ok {
			return
		}
		var req struct {
			TunnelID string `json:"tunnel_id"`
			Data     string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, map[string]string{"error": err.Error()})
			return
		}
		data, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			writeJSON(w, map[string]string{"error": "bad base64"})
			return
		}
		msg := protocol.NewMessage(protocol.TypeTunnelData, protocol.TunnelDataPayload{TunnelID: req.TunnelID, Data: data})
		hub.handleHTTPMessage(child, &msg)
		writeJSON(w, map[string]bool{"ok": true})
	})
}
