package mother

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

// Tunnel represents an active port-forward tunnel.
type Tunnel struct {
	ID         string
	ChildID    string
	Target     string
	ListenPort int
	CreatedAt  time.Time
	BytesIn    uint64
	BytesOut   uint64
	mu         sync.Mutex
	cancel     chan struct{}
}

// TunnelManager manages tunnels.
type TunnelManager struct {
	mu      sync.RWMutex
	tunnels map[string]*Tunnel
	hub     *Hub
}

// NewTunnelManager creates a tunnel manager.
func NewTunnelManager(hub *Hub) *TunnelManager {
	return &TunnelManager{
		tunnels: make(map[string]*Tunnel),
		hub:     hub,
	}
}

// OpenTunnel starts a TCP listener and forwards traffic through the child's WS.
func (tm *TunnelManager) OpenTunnel(id, childID, target string, listenPort int) (*Tunnel, error) {
	tm.hub.mu.RLock()
	child, ok := tm.hub.children[childID]
	tm.hub.mu.RUnlock()
	if !ok {
		return nil, ErrChildNotFound
	}

	t := &Tunnel{
		ID:         id,
		ChildID:    childID,
		Target:     target,
		ListenPort: listenPort,
		CreatedAt:  time.Now(),
		cancel:     make(chan struct{}),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", listenPort))
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	tm.mu.Lock()
	tm.tunnels[id] = t
	tm.mu.Unlock()

	go func() {
		defer listener.Close()
		defer func() {
			tm.mu.Lock()
			delete(tm.tunnels, id)
			tm.mu.Unlock()
		}()

		for {
			select {
			case <-t.cancel:
				return
			default:
			}

			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-t.cancel:
					return
				default:
					log.Printf("[tunnel] %s accept error: %v", id, err)
					continue
				}
			}
			go tm.handleTunnelConn(child, id, conn, t)
		}
	}()

	log.Printf("[tunnel] %s opened: :%d -> %s@%s", id, listenPort, childID, target)
	return t, nil
}

func (tm *TunnelManager) handleTunnelConn(child *ChildState, tunnelID string, conn net.Conn, t *Tunnel) {
	defer conn.Close()

	// Read from TCP -> Send to child via WS tunnel_data
	go func() {
		buf := make([]byte, 32*1024)
		for {
			select {
			case <-t.cancel:
				return
			default:
			}
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			data := make([]byte, n)
			copy(data, buf[:n])

			msg := protocol.NewMessage(protocol.TypeTunnelData, protocol.TunnelDataPayload{
				TunnelID: tunnelID,
				Data:     data,
			})
			child.mu.Lock()
			b, _ := msgpack.Marshal(msg)
			child.Conn.WriteMessage(websocket.BinaryMessage, b)
			child.mu.Unlock()

			t.mu.Lock()
			t.BytesIn += uint64(n)
			t.mu.Unlock()
		}
	}()

	// Keep connection alive; data from child comes via hub -> would need a reverse channel
	// For v1, tunnel is one-directional (inbound only)
	<-t.cancel
}

// CloseTunnel closes a tunnel.
func (tm *TunnelManager) CloseTunnel(id string) error {
	tm.mu.Lock()
	t, ok := tm.tunnels[id]
	tm.mu.Unlock()
	if !ok {
		return fmt.Errorf("tunnel not found")
	}
	close(t.cancel)
	return nil
}

// ListTunnels returns all active tunnels.
func (tm *TunnelManager) ListTunnels() []TunnelInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var list []TunnelInfo
	for _, t := range tm.tunnels {
		t.mu.Lock()
		list = append(list, TunnelInfo{
			ID:         t.ID,
			ChildID:    t.ChildID,
			Target:     t.Target,
			ListenPort: t.ListenPort,
			BytesIn:    t.BytesIn,
			BytesOut:   t.BytesOut,
		})
		t.mu.Unlock()
	}
	return list
}

type TunnelInfo struct {
	ID         string `json:"id"`
	ChildID    string `json:"child_id"`
	Target     string `json:"target"`
	ListenPort int    `json:"listen_port"`
	BytesIn    uint64 `json:"bytes_in"`
	BytesOut   uint64 `json:"bytes_out"`
}

type TunnelRequest struct {
	ID         string `json:"id"`
	ChildID    string `json:"child_id"`
	Target     string `json:"target"`
	ListenPort int    `json:"listen_port"`
}

// startTunnel is called from hub when child sends tunnel_open.
func (h *Hub) startTunnel(child *ChildState, payload *protocol.TunnelOpenPayload) {
	// This would need the TunnelManager from the mother instance.
	// For now, just acknowledge.
	log.Printf("[hub] tunnel %s acknowledged for child %s", payload.TunnelID, child.ID)
}

// WriteSSE streams events as Server-Sent Events.
func WriteSSE(w http.ResponseWriter, events <-chan interface{}) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}

	for evt := range events {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
}

// DiscardRead is a helper for tunnels.
var _ = io.Discard